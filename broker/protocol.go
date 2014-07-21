package main

import (
	"bytes"
	"net"
	"strconv"
	// "strings"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"code.sohuno.com/kzapp/push-server/model"
	"code.sohuno.com/kzapp/push-server/util"
)

const maxTimeout = time.Hour

var separatorBytes = []byte(" ")
var heartbeatBytes = []byte("_heartbeat_")

type protocol struct {
	context *context
}

func (p *protocol) IOLoop(conn net.Conn) error {
	var err error
	var zeroTime time.Time
	var byteBuf bytes.Buffer

	client := newClient(conn, p.context)

	messagePumpStartedChan := make(chan bool)
	go p.messagePump(client, messagePumpStartedChan)
	<-messagePumpStartedChan

	for {
		// log.Printf("client[%s] HeartbeatInterval %d ", client, client.HeartbeatInterval)
		if client.HeartbeatInterval > 0 {
			client.SetReadDeadline(time.Now().Add(client.HeartbeatInterval * 2))
		} else {
			client.SetReadDeadline(zeroTime)
		}

		cmd, err := ParseCommand(client.Reader)
		if err != nil {
			log.Printf("parse command error %s", err.Error())
			break
		}

		// if p.context.broker.options.Verbose {
		// log.Printf("PROTOCOL(V1): [%d] %s", client.ClientID, cmd)
		// }

		response, err := p.Exec(client, cmd)
		if err != nil {
			context := ""
			if parentErr := err.(util.ChildErr).Parent(); parentErr != nil {
				context = " - " + parentErr.Error()
			}
			log.Printf("[%s] - %s %s", client, err.Error(), context)

			errResponse := &Response{false, []byte(err.Error())}
			sendErr := p.SendResponse(client, errResponse, &byteBuf)
			if sendErr != nil {
				break
			}

			// errors of type FatalClientErr should forceably close the connection
			if _, ok := err.(*util.FatalClientErr); ok {
				break
			}
			continue
		}

		if response != nil {
			err = p.SendResponse(client, response, &byteBuf)
			if err != nil {
				log.Printf("send response to client error %s ", err)
				break
			}
		}
	}

	log.Printf("PROTOCOL: [%s] exiting ioloop", client)
	p.cleanupClientConn(client)
	return err
}

func (p *protocol) cleanupClientConn(client *client) {

	p.context.broker.RemoveClient(client.ClientID) //, client.SubTopic)

	client.stopper.Do(func() {
		atomic.StoreInt32(&client.stopFlag, 1)

		client.Close()
		model.DelClientConn(client.ClientID)

		// if client.SubTopic != "" {
		// }
		// touch devie online
		model.TouchDeviceOffline(client.ClientID)
		close(client.ExitChan)
	})
}

func (p *protocol) SendMessage(client *client, msg *model.Message, buf *bytes.Buffer) error {
	if p.context.broker.options.Verbose {
		// log.Printf("PROTOCOL: writing msg to client(%s) msg_id_%s - %s", client, msg.ID, msg.Body)
	}

	cmd := Pub(msg.ID, []byte(msg.Body))

	buf.Reset()
	err := cmd.Write(buf)
	if err != nil {
		return err
	}

	client.Lock()
	client.SetWriteDeadline(time.Now().Add(time.Second))
	_, err = client.Writer.Write(buf.Bytes())
	if err != nil {
		client.Unlock()
		return err
	}
	client.Unlock()
	return err
}

func (p *protocol) SendResponse(client *client, resp *Response, buf *bytes.Buffer) error {
	// log.Printf("protol  send response %s", data)

	buf.Reset()
	err := resp.Write(buf)
	if err != nil {
		return err
	}

	client.Lock()

	client.SetWriteDeadline(time.Now().Add(time.Second))
	_, err = client.Writer.Write(buf.Bytes())
	if err != nil {
		client.Unlock()
		return err
	}

	err = client.Flush()
	client.Unlock()
	return err
}

func (p *protocol) Exec(client *client, cmd *Command) (*Response, error) {
	switch {
	case bytes.Equal(cmd.Name, []byte("H")):
		return p.HEARTBEAT(client, cmd)
	case bytes.Equal(cmd.Name, []byte("CONN")):
		return p.CONN(client, cmd)
	case bytes.Equal(cmd.Name, []byte("SUB")):
		return p.SUB(client, cmd)
	case bytes.Equal(cmd.Name, []byte("PUB")):
		return p.PUB(client, cmd)
	}

	log.Printf("parse cmd (%s) error, line %s ", client, cmd)
	return nil, util.NewFatalClientErr(nil, "invalid_command", fmt.Sprintf("invalid command %s", cmd))
}

func (p *protocol) CONN(client *client, cmd *Command) (*Response, error) {
	var err error

	if atomic.LoadInt32(&client.State) != StateInit {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "cannot CONN in current state")
	}

	// log.Printf("PROTOCOL: [%s] %s", client, cmd)

	client_id, err := strconv.ParseInt(string(cmd.Params[0]), 10, 64)
	if err != nil {
		return nil, util.NewFatalClientErr(err, "invalid_client_id", "CONN "+err.Error())
	}
	client.ClientID = client_id

	heartbeat_interval, err := strconv.Atoi(string(cmd.Params[1]))
	if err != nil {
		return nil, util.NewFatalClientErr(err, "invalid_heartbeat_interval", "CONN "+err.Error())
	}
	client.SetHeartbeatInterval(heartbeat_interval)

	// should send client connected eventsf
	// log.Printf("SetClientConn clientID=%d, broker_addr=%s", client.ClientID, p.context.broker.options.BroadcastAddress)
	err = model.SetClientConn(client.ClientID, p.context.broker.options.BroadcastAddress)
	if err != nil {
		log.Printf("setClientConn error %s", err.Error())
		return nil, util.NewFatalClientErr(nil, "conn_failed", "set client conn error")
	}

	p.context.broker.AddClient(client.ClientID, client)
	// log.Printf("clientId %d conn success ", client.ClientID)

	// go p.checkOfflineMessage(client)
	// log.Printf("PROTOCOL: [%s] identify finish!", client)

	return RespConnAck, nil
}

func (p *protocol) SUB(client *client, cmd *Command) (*Response, error) {

	if atomic.LoadInt32(&client.State) != StateInit {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "cannot SUB in current state")
	}

	if client.HeartbeatInterval <= 0 {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "cannot SUB with heartbeats disabled")
	}

	// log.Printf("receive params on sub  %s", cmd)
	if len(cmd.Params) < 1 {
		return nil, util.NewFatalClientErr(nil, "invalid_sub_command", "SUB insufficient number of parameters")
	}

	topic_id := string(cmd.Params[0])
	if len(topic_id) == 0 {
		return nil, util.NewFatalClientErr(nil, "invalid_topic_id", "invalid topic id ")
	}

	client_id := client.ClientID

	// ok := model.CheckOrCreateChannel(topic_id)
	// if ok == false {
	// 	return nil, util.NewFatalClientErr(nil, "E_INVALID", "create topic error")
	// }

	// device, err := model.FindDeviceByID(client_id)
	// if err != nil || device == nil {
	// 	log.Printf("invalid client id [%d] err: %s", client_id, err)
	// 	return nil, util.NewFatalClientErr(nil, "E_INVALID", "invalid client id ")
	// }

	//TODO send subscribe event
	sub_id := <-p.context.broker.idChan
	sub := &model.Subscribe{
		ID:        sub_id,
		ChannelID: topic_id,
		DeviceID:  client_id,
		// DeviceType: device.DeviceType,
		CreatedAt: time.Now().UnixNano(),
		UpdatedAt: time.Now().UnixNano(),
	}

	err := model.SaveOrUpdateSubscribe(sub)
	if err != nil {
		log.Printf("SaveOrUpdateSubscribe err  [%d] : %s", client_id, err)
		return nil, util.NewFatalClientErr(nil, "internal error", "save subscribe error")
	}
	// log.Printf("clientId %d save sub topic %s ", client.ClientID, topic_id)

	// p.context.broker.AddClient(client.ClientID, topic_id, client)
	// log.Printf("clientId %d sub topic %d success ", client.ClientID, topic_id)

	// touch devie online
	model.TouchDeviceOnline(client_id)

	// increase channel sub count
	// add client to channel sub list

	atomic.StoreInt32(&client.State, StateSubscribed)
	client.SubTopic = topic_id

	// client.Channel = channel
	// update message pump
	// client.SubEventChan <- channel

	return RespSubAck, nil
}

func (p *protocol) messagePump(client *client, startedChan chan bool) {
	var err error
	var buf bytes.Buffer
	// NOTE: `flusherChan` is used to bound message latency for
	// the pathological case of a channel on a low volume topic
	// with >1 clients having >1 RDY counts
	var flusherChan <-chan time.Time

	outputBufferTimeout := 250 * time.Millisecond
	outputBufferTicker := time.NewTicker(outputBufferTimeout)

	// v2 opportunistically buffers data to clients to reduce write system calls
	// we force flush in two cases:
	//    1. when the client is not ready to receive messages
	//    2. we're buffered and the channel has nothing left to send us
	//       (ie. we would block in this loop anyway)
	//
	flushed := true

	// signal to the goroutine that started the messagePump
	// that we've started up
	close(startedChan)

	for {
		// log.Printf("enter forloop")
		if atomic.LoadInt32(&client.stopFlag) == 1 {
			// the client is not ready to receive messages...
			flusherChan = nil
			goto exit
		} else if flushed {
			// last iteration we flushed...
			// do not select on the flusher ticker channel
			flusherChan = nil
		} else {
			// we're buffered (if there isn't any more data we should flush)...
			// select on the flusher ticker channel, too
			flusherChan = outputBufferTicker.C
		}
		// log.Printf("before select")
		select {
		case <-flusherChan:
			// log.Printf("enter flusherChan")
			// if this case wins, we're either starved
			// or we won the race between other channels...
			// in either case, force flush
			client.Lock()
			err = client.Flush()
			client.Unlock()
			if err != nil {
				goto exit
			}
			flushed = true
			// log.Printf("leave flusherChan")
		case msg, ok := <-client.clientMsgChan:
			// log.Printf("get client message %s", msg.Id)
			if !ok {
				goto exit
			}

			// client.SendingMessage()
			// log.Printf("before send message %s", msg.Id)
			err = p.SendMessage(client, msg, &buf)
			// log.Printf("after send message %s", msg.Id)
			if err != nil {
				atomic.AddUint64(&p.context.broker.ErrorCount, 1)
				goto exit
			}
			atomic.AddUint64(&p.context.broker.FinishedCount, 1)
			flushed = false
			// log.Printf("send message finish %s \n", msg.Id)
		case <-client.ExitChan:
			goto exit
		}
		// log.Printf("after select")
		// log.Printf("leave fooloop")
	}

exit:
	log.Printf("PROTOCOL: [%s] exiting messagePump", client)
	outputBufferTicker.Stop()
	if err != nil {
		log.Printf("PROTOCOL: [%s] messagePump error - %s", client, err.Error())
	}
}

func (p *protocol) checkOfflineMessage(client *client) {

	log.Printf("start GetOfflineMessages clientID %d ", client.ClientID)

	messageIDs, err := model.GetOfflineMessages(client.ClientID)
	if err != nil {
		log.Printf("GetOfflineMessages clientID %d error %d ", client.ClientID, err)
		return
	}

	log.Printf("GetOfflineMessages clientID %d msgIDs %s", client.ClientID, messageIDs)
	if messageIDs == nil {
		return
	}

	// subChannel := client.SubTopic
	for _, messageID := range messageIDs {
		message, err := model.FindMessageByID(messageID)
		if err != nil || message == nil {
			log.Printf("client %s message ID %d message doesn't exist, err %s", client.ClientID, messageID, err)
			continue
		}

		// live := msg.CreatedAt + msg.Expires*100000000000000
		// if time.Now().UnixNano() > live {
		// 	log.Printf("client %s message ID %d message expired.", client.ClientID, messageID)
		// 	model.RemoveOfflineMessage(client.ClientID, messageID)
		// 	continue
		// }

		// if subChannel != message.ChannelID {
		// 	continue
		// }

		// log.Printf("output_offline_msg %d", client.ClientID)

		client.clientMsgChan <- message
		model.RemoveOfflineMessage(client.ClientID, messageID)
	}
}

// hearbeat
func (p *protocol) HEARTBEAT(client *client, cmd *Command) (*Response, error) {
	// log.Printf("[%s] heartbeat received", client)
	return RespHeartbeat, nil
}

func (p *protocol) PUB(client *client, cmd *Command) (*Response, error) {

	if len(cmd.Params) < 3 {
		return nil, util.NewFatalClientErr(nil, "invalid_pub_command", "PUB insufficient number of parameters")
	}

	// log.Printf("receive params on sub  %s", params)
	topic_id := string(cmd.Params[0])
	if len(topic_id) == 0 {
		return nil, util.NewFatalClientErr(nil, "invalid_topic_id", "PUB insufficient number of parameters")
	}

	// msg_id := string(cmd.Params[1])
	body := string(cmd.Params[2])
	bodyLen := len(body)
	if int64(bodyLen) > p.context.broker.options.MaxBodySize {
		return nil, util.NewFatalClientErr(nil, "pub_body_too_big", fmt.Sprintf("PUB body too big %d > %d", bodyLen, p.context.broker.options.MaxBodySize))
	}

	if int64(bodyLen) <= 0 {
		return nil, util.NewFatalClientErr(nil, "E_BAD_BODY", "PUB failed to read body")
	}

	// client_id, _ := strconv.ParseInt(string(params[1]), 10, 64)
	// topic_id := string(params[2])
	// message_id, _ := strconv.ParseInt(string(params[3]), 10, 64)

	msg := &model.Message{
		ID:        <-p.context.broker.idChan,
		ChannelID: topic_id,
		CreatedAt: time.Now().UnixNano(),
		Body:      string(body),
		// PushType:   int8(push_type),
		DeviceType: int8(model.ALLDevice),
	}

	err := p.context.broker.PutMessage(msg)
	if err != nil {
		return nil, util.NewFatalClientErr(nil, "put_error", "PUB insufficient number of parameters")
	}

	return &Response{true, []byte("PUBACK")}, nil
}
