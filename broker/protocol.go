package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	// "strconv"
	// "strings"
	"sync/atomic"
	"time"

	"code.sohuno.com/kzapp/push-server/model"
	"code.sohuno.com/kzapp/push-server/util"
)

const maxTimeout = time.Hour

var separatorBytes = []byte(" ")
var heartbeatBytes = []byte("_heartbeat_")
var identifyOKBytes = []byte("IDENTIFY OK")
var subOkBytes = []byte("SUBACK")

type protocol struct {
	context *context
}

func (p *protocol) IOLoop(conn net.Conn) error {
	var err error
	var line []byte
	var zeroTime time.Time

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

		// ReadSlice does not allocate new space for the data each request
		// ie. the returned slice is only valid until the next call to it
		line, err = client.Reader.ReadBytes('\n')
		if err != nil {
			log.Printf(err.Error())
			break
		}

		// trim the '\n'
		line = line[:len(line)-1]
		// optionally trim the '\r'
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		// log.Printf("protocol receive line <%s>", line)

		params := bytes.Split(line, separatorBytes)

		if p.context.broker.options.Verbose {
			log.Printf("PROTOCOL(V1): [%s] %s", client, params)
		}

		response, err := p.Exec(client, params)
		if err != nil {
			context := ""
			if parentErr := err.(util.ChildErr).Parent(); parentErr != nil {
				context = " - " + parentErr.Error()
			}
			log.Printf("[%s] - %s %s", client, err.Error(), context)

			sendErr := p.Send(client, util.FrameTypeError, []byte(err.Error()))
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
			err = p.Send(client, util.FrameTypeResponse, response)
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

	p.context.broker.RemoveClient(client.ClientID) //, client.SubChannel)

	client.stopper.Do(func() {
		atomic.StoreInt32(&client.stopFlag, 1)

		client.Close()
		model.DelClientConn(client.ClientID)

		// if client.SubChannel != "" {
		// }
		// touch devie online
		model.TouchDeviceOffline(client.ClientID)
		close(client.ExitChan)
	})
}

func (p *protocol) SendMessage(client *client, msg *Message, buf *bytes.Buffer) error {
	if p.context.broker.options.Verbose {
		log.Printf("PROTOCOL: writing msg(%s) to client(%s) - %s",
			msg.Id, client, msg.Body)
	}

	buf.Reset()
	err := msg.Write(buf)
	if err != nil {
		return err
	}

	err = p.Send(client, util.FrameTypeMessage, buf.Bytes())
	if err != nil {
		return err
	}

	// log.Printf("PROTOCOL: Success writing msg(%s) to client(%s) - %s", msg.Id, client, msg.Body)
	return nil
}

func (p *protocol) Send(client *client, frameType int32, data []byte) error {
	// log.Printf("protol  send response %s", data)
	client.Lock()

	client.SetWriteDeadline(time.Now().Add(time.Second))
	_, err := util.SendFramedResponse(client.Writer, frameType, data)
	if err != nil {
		client.Unlock()
		return err
	}

	if frameType != util.FrameTypeMessage {
		err = client.Flush()
	}

	client.Unlock()

	return err
}

func (p *protocol) Exec(client *client, params [][]byte) ([]byte, error) {
	switch {
	case bytes.Equal(params[0], []byte("H")):
		return p.HEARTBEAT(client, params)
	// case bytes.Equal(params[0], []byte("PUB")):
	// 	return p.PUB(client, params)
	case bytes.Equal(params[0], []byte("NOP")):
		return p.NOP(client, params)
	case bytes.Equal(params[0], []byte("IDENTIFY")):
		return p.IDENTIFY(client, params)
	case bytes.Equal(params[0], []byte("SUB")):
		return p.SUB(client, params)
	case bytes.Equal(params[0], []byte("CLS")):
		return p.CLS(client, params)
	}

	log.Printf("parse cmd (%s) error, line %s ", params[0], params)
	return nil, util.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}

func (p *protocol) IDENTIFY(client *client, params [][]byte) ([]byte, error) {
	var err error

	if atomic.LoadInt32(&client.State) != StateInit {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "cannot IDENTIFY in current state")
	}

	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, util.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body size")
	}

	if int64(bodyLen) > p.context.broker.options.MaxBodySize {
		return nil, util.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("IDENTIFY body too big %d > %d", bodyLen, p.context.broker.options.MaxBodySize))
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, body)
	log.Printf("identify body %s", string(body))
	if err != nil {
		return nil, util.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
	}

	// body is a json structure with producer information
	var identifyData identifyDataV2
	err = json.Unmarshal(body, &identifyData)
	if err != nil {
		return nil, util.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")
	}

	if p.context.broker.options.Verbose {
		log.Printf("PROTOCOL: [%s] %+v", client, identifyData)
	}

	err = client.Identify(identifyData)
	if err != nil {
		return nil, util.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY "+err.Error())
	}

	// should send client connected eventsf
	log.Printf("SetClientConn clientID=%d, broker_addr=%s", client.ClientID, p.context.broker.options.BroadcastAddress)
	err = model.SetClientConn(client.ClientID, p.context.broker.options.BroadcastAddress)
	if err != nil {
		log.Printf("setClientConn" + err.Error())
		return nil, util.NewFatalClientErr(nil, "E_IDENTIFY_FAILED", "set client conn error")
	}

	p.context.broker.AddClient(client.ClientID, client)
	log.Printf("clientId %d conn success ", client.ClientID)

	go p.checkOfflineMessage(client)
	log.Printf("PROTOCOL: [%s] identify finish!", client)

	return identifyOKBytes, nil
}

func (p *protocol) SUB(client *client, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != StateInit {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "cannot SUB in current state")
	}

	if client.HeartbeatInterval <= 0 {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "cannot SUB with heartbeats disabled")
	}

	log.Printf("receive params on sub  %s", params)
	if len(params) < 1 {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "SUB insufficient number of parameters")
	}

	channel_id := string(params[1])
	// if err != nil {
	// 	return nil, util.NewFatalClientErr(nil, "E_INVALID", "invalid channel id ")
	// }
	client_id := client.ClientID

	// ok := model.CheckOrCreateChannel(channel_id)
	// if ok == false {
	// 	return nil, util.NewFatalClientErr(nil, "E_INVALID", "create channel error")
	// }
	device, err := model.FindDeviceByID(client_id)
	if err != nil || device == nil {
		log.Printf("invalid client id [%d] err: %s", client_id, err)
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "invalid client id ")
	}

	//TODO send subscribe event
	sub_id := <-p.context.broker.idChan
	sub := &model.Subscribe{
		ID:         sub_id,
		ChannelID:  channel_id,
		DeviceID:   client_id,
		DeviceType: device.DeviceType,
		CreatedAt:  time.Now().UnixNano(),
		UpdatedAt:  time.Now().UnixNano(),
	}
	err = model.SaveOrUpdateSubscribe(sub)
	if err != nil {
		log.Printf("SaveOrUpdateSubscribe err  [%d] : %s", client_id, err)
		return nil, util.NewFatalClientErr(nil, "internal error", "save subscribe error")
	}
	log.Printf("clientId %d save sub channel %s ", client.ClientID, channel_id)

	// p.context.broker.AddClient(client.ClientID, channel_id, client)
	// log.Printf("clientId %d sub channel %d success ", client.ClientID, channel_id)

	// touch devie online
	model.TouchDeviceOnline(client_id)

	// increase channel sub count
	// add client to channel sub list

	atomic.StoreInt32(&client.State, StateSubscribed)
	client.SubChannel = channel_id

	// client.Channel = channel
	// update message pump
	// client.SubEventChan <- channel

	return subOkBytes, nil
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

	// subChannel := client.SubChannel
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

		msg := &Message{
			Id:        util.Guid(message.ID).Hex(),
			Body:      []byte(message.Body),
			Timestamp: message.CreatedAt,
		}
		// log.Printf("output_offline_msg %d", client.ClientID)

		client.clientMsgChan <- msg
		model.RemoveOfflineMessage(client.ClientID, messageID)
	}
}

// hearbeat
func (p *protocol) HEARTBEAT(client *client, params [][]byte) ([]byte, error) {
	// log.Printf("[%s] heartbeat received", client)
	return []byte("H"), nil
}

func (p *protocol) CLS(client *client, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != StateSubscribed {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "cannot CLS in current state")
	}

	client.StartClose()
	return []byte("CLOSE_WAIT"), nil
}

func (p *protocol) NOP(client *client, params [][]byte) ([]byte, error) {
	return nil, nil
}

func (p *protocol) PUB(client *client, params [][]byte) ([]byte, error) {
	// var err error
	if client.Role != "$_@push_sign_$_kz_worker" {
		return nil, util.NewFatalClientErr(nil, "E_INVALID_REQUEST", "client can't pub message")
	}

	if len(params) < 3 {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "PUB insufficient number of parameters")
	}
	// log.Printf("receive params on sub  %s", params)

	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, util.NewFatalClientErr(err, "E_BAD_BODY", "PUB failed to read body size")
	}

	if int64(bodyLen) > p.context.broker.options.MaxBodySize {
		return nil, util.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("PUB body too big %d > %d", bodyLen, p.context.broker.options.MaxBodySize))
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, body)
	if err != nil {
		return nil, util.NewFatalClientErr(err, "E_BAD_BODY", "PUB failed to read body")
	}

	// client_id, _ := strconv.ParseInt(string(params[1]), 10, 64)
	// channel_id := string(params[2])
	// message_id, _ := strconv.ParseInt(string(params[3]), 10, 64)
	// atomic.AddUint64(&p.context.broker.MessageCount, 1)

	// p.context.broker.pubChan <- &PubMessage{clientID: client_id,
	// 	messageID: message_id,
	// 	channelID: channel_id,
	// 	pubClient: client,
	// 	body:      body}
	// log.Printf("receive params on sub  %s", params)
	return nil, nil
}

// func AckPublish(ackType int32, clientID int64, msgID int64) []byte {
// 	response := []byte(fmt.Sprintf("%d %d %d", ackType, clientID, msgID))
// 	return response
// }

func readLen(r io.Reader, tmp []byte) (int32, error) {
	_, err := io.ReadFull(r, tmp)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(tmp)), nil
}
