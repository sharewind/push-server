package broker

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"code.sohuno.com/kzapp/push-server/model"
	"code.sohuno.com/kzapp/push-server/util"
)

const maxTimeout = time.Hour

var separatorBytes = []byte(" ")
var heartbeatBytes = []byte("_heartbeat_")
var okBytes = []byte("OK")

type protocol struct {
	context *context
}

func (p *protocol) IOLoop(conn net.Conn) error {
	var err error
	var line []byte
	var zeroTime time.Time

	client := newClient(conn, p.context)

	for {
		// log.Info("client[%s] HeartbeatInterval %d ", client, client.HeartbeatInterval)
		if client.HeartbeatInterval > 0 {
			client.SetReadDeadline(time.Now().Add(client.HeartbeatInterval * 2))
		} else {
			client.SetReadDeadline(zeroTime)
		}

		// ReadSlice does not allocate new space for the data each request
		// ie. the returned slice is only valid until the next call to it
		line, err = client.Reader.ReadSlice('\n')
		if err != nil {
			break
		}

		// trim the '\n'
		line = line[:len(line)-1]
		// optionally trim the '\r'
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		params := bytes.Split(line, separatorBytes)

		if p.context.broker.options.Verbose {
			log.Debug("PROTOCOL(V1): [%s] %s", client, params)
		}

		response, err := p.Exec(client, params)
		if err != nil {
			context := ""
			if parentErr := err.(util.ChildErr).Parent(); parentErr != nil {
				context = " - " + parentErr.Error()
			}
			log.Error("[%s] - %s%s", client, err.Error(), context)

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
				log.Error("send response to client error %s ", err)
				break
			}
		}
	}

	log.Debug("PROTOCOL: [%s] exiting ioloop", client)
	p.cleanupClientConn(client)
	return err
}

func (p *protocol) cleanupClientConn(client *client) {

	client.stopper.Do(func() {
		atomic.StoreInt32(&client.stopFlag, 1)

		client.Close()
		model.DelClientConn(client.ClientID)

		if client.SubChannel != 0 && client.SubChannel != -1 {
			p.context.broker.RemoveClient(client.ClientID, client.SubChannel)
		}
		// touch devie online
		model.TouchDeviceOffline(client.ClientID)
		close(client.ExitChan)
	})
}

func (p *protocol) SendMessage(client *client, msg *Message, buf *bytes.Buffer) error {
	// if p.context.broker.options.Verbose {
	// log.Debug("PROTOCOL: writing msg(%s) to client(%s) - %s",
	// msg.Id, client, msg.Body)
	// }

	buf.Reset()
	err := msg.Write(buf)
	if err != nil {
		return err
	}

	err = p.Send(client, util.FrameTypeMessage, buf.Bytes())
	if err != nil {
		return err
	}

	log.Debug("PROTOCOL: Success writing msg(%s) to client(%s) - %s", msg.Id, client, msg.Body)
	return nil
}

func (p *protocol) Send(client *client, frameType int32, data []byte) error {
	client.Lock()

	client.SetWriteDeadline(time.Now().Add(time.Second * 10))
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
	case bytes.Equal(params[0], []byte("PUB")):
		return p.PUB(client, params)
	case bytes.Equal(params[0], []byte("NOP")):
		return p.NOP(client, params)
	case bytes.Equal(params[0], []byte("IDENTIFY")):
		return p.IDENTIFY(client, params)
	case bytes.Equal(params[0], []byte("SUB")):
		return p.SUB(client, params)
	case bytes.Equal(params[0], []byte("CLS")):
		return p.CLS(client, params)
	}
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
		log.Debug("PROTOCOL: [%s] %+v", client, identifyData)
	}

	err = client.Identify(identifyData)
	if err != nil {
		return nil, util.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY "+err.Error())
	}

	// bail out early if we're not negotiating features
	if !identifyData.FeatureNegotiation {
		return okBytes, nil
	}

	tlsv1 := p.context.broker.tlsConfig != nil && identifyData.TLSv1
	deflate := p.context.broker.options.DeflateEnabled && identifyData.Deflate
	deflateLevel := 0
	if deflate {
		if identifyData.DeflateLevel <= 0 {
			deflateLevel = 6
		}
		deflateLevel = int(math.Min(float64(deflateLevel), float64(p.context.broker.options.MaxDeflateLevel)))
	}
	snappy := p.context.broker.options.SnappyEnabled && identifyData.Snappy

	if deflate && snappy {
		return nil, util.NewFatalClientErr(nil, "E_IDENTIFY_FAILED", "cannot enable both deflate and snappy compression")
	}

	resp, err := json.Marshal(struct {
		Version         string `json:"version"`
		TLSv1           bool   `json:"tls_v1"`
		Deflate         bool   `json:"deflate"`
		DeflateLevel    int    `json:"deflate_level"`
		MaxDeflateLevel int    `json:"max_deflate_level"`
		Snappy          bool   `json:"snappy"`
		SampleRate      int32  `json:"sample_rate"`
	}{
		Version:         util.BINARY_VERSION,
		TLSv1:           tlsv1,
		Deflate:         deflate,
		DeflateLevel:    deflateLevel,
		MaxDeflateLevel: p.context.broker.options.MaxDeflateLevel,
		Snappy:          snappy,
		SampleRate:      client.SampleRate,
	})
	if err != nil {
		return nil, util.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	}

	err = p.Send(client, util.FrameTypeResponse, resp)
	if err != nil {
		return nil, util.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	}

	if tlsv1 {
		log.Debug("PROTOCOL: [%s] upgrading connection to TLS", client)
		err = client.UpgradeTLS()
		if err != nil {
			return nil, util.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, util.FrameTypeResponse, okBytes)
		if err != nil {
			return nil, util.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	if snappy {
		log.Debug("PROTOCOL: [%s] upgrading connection to snappy", client)
		err = client.UpgradeSnappy()
		if err != nil {
			return nil, util.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, util.FrameTypeResponse, okBytes)
		if err != nil {
			return nil, util.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	if deflate {
		log.Debug("PROTOCOL: [%s] upgrading connection to deflate", client)
		err = client.UpgradeDeflate(deflateLevel)
		if err != nil {
			return nil, util.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, util.FrameTypeResponse, okBytes)
		if err != nil {
			return nil, util.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	return nil, nil
}

func (p *protocol) SUB(client *client, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != StateInit {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "cannot SUB in current state")
	}

	if client.HeartbeatInterval <= 0 {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "cannot SUB with heartbeats disabled")
	}

	log.Debug("receive params on sub  %s", params)
	if len(params) < 1 {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "SUB insufficient number of parameters")
	}

	channel_id, err := strconv.ParseInt(string(params[1]), 10, 64)
	if err != nil {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "invalid channel id ")
	}
	client_id := client.ClientID
	if err != nil {
		log.Error("invalid client id [%s] err: %d", client.ClientID, err)
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "invalid client id ")
	}

	// ok := model.CheckOrCreateChannel(channel_id)
	// if ok == false {
	// 	return nil, util.NewFatalClientErr(nil, "E_INVALID", "create channel error")
	// }
	device, err := model.FindDeviceByID(client_id)
	if err != nil || device == nil {
		log.Error("invalid client id [%d] err: %s", client_id, err)
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "invalid client id ")
	}

	//TODO send subscribe event
	sub := &model.Subscribe{
		ChannelID:  channel_id,
		DeviceID:   client_id,
		DeviceType: device.DeviceType,
		CreatedAt:  time.Now().UnixNano(),
		UpdatedAt:  time.Now().UnixNano(),
	}
	err = model.SaveOrUpdateSubscribe(sub)
	if err != nil {
		log.Error("SaveOrUpdateSubscribe err  [%d] : %s", client_id, err)
		return nil, util.NewFatalClientErr(nil, "internal error", "save subscribe error")
	}
	log.Info("clientId %d save sub channel %d ", client.ClientID, channel_id)

	p.context.broker.AddClient(client.ClientID, channel_id, client)
	log.Info("clientId %d sub channel %d success ", client.ClientID, channel_id)

	// touch devie online
	model.TouchDeviceOnline(client_id)

	// should send client connected eventsf
	log.Info("SetClientConn clientID=%d, broker_addr=%s", client.ClientID, p.context.broker.options.BroadcastAddress)
	err = model.SetClientConn(client.ClientID, p.context.broker.options.BroadcastAddress)
	if err != nil {
		log.Error(err.Error())
		return nil, util.NewFatalClientErr(nil, "internal error", "save subscribe error")
	}

	// increase channel sub count
	// add client to channel sub list

	// topic := p.context.broker.GetTopic(topicName)
	// channel := topic.GetChannel(channelName)
	// channel.AddClient(client.ID, client)

	atomic.StoreInt32(&client.State, StateSubscribed)
	client.SubChannel = channel_id

	messagePumpStartedChan := make(chan bool)
	go p.messagePump(client, messagePumpStartedChan)
	<-messagePumpStartedChan

	go p.checkOfflineMessage(client)
	// client.Channel = channel
	// update message pump
	// client.SubEventChan <- channel

	return okBytes, nil
}

func (p *protocol) messagePump(client *client, startedChan chan bool) {
	var err error
	var buf bytes.Buffer
	// NOTE: `flusherChan` is used to bound message latency for
	// the pathological case of a channel on a low volume topic
	// with >1 clients having >1 RDY counts
	var flusherChan <-chan time.Time
	outputBufferTicker := time.NewTicker(client.OutputBufferTimeout)

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

		select {
		case <-flusherChan:
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
		case msg, ok := <-client.clientMsgChan:
			if !ok {
				goto exit
			}

			// client.SendingMessage()
			err = p.SendMessage(client, msg, &buf)
			if err != nil {
				goto exit
			}
			flushed = false
		case <-client.ExitChan:
			goto exit
		}
	}

exit:
	log.Debug("PROTOCOL: [%s] exiting messagePump", client)
	outputBufferTicker.Stop()
	if err != nil {
		log.Error("PROTOCOL: [%s] messagePump error - %s", client, err.Error())
	}
}

func (p *protocol) checkOfflineMessage(client *client) {

	messageIDs, err := model.GetOfflineMessages(client.ClientID)
	if err != nil {
		log.Error("GetOfflineMessages clientID %d error %d ", client.ClientID, err)
		return
	}
	if messageIDs == nil {
		return
	}

	subChannel := client.SubChannel
	for _, messageID := range messageIDs {
		message, err := model.FindMessageByID(messageID)
		if err != nil || message == nil {
			log.Error("client %s message ID %d message doesn't exist, err %s", client.ClientID, messageID, err)
			continue
		}

		// live := msg.CreatedAt + msg.Expires*100000000000000
		// if time.Now().UnixNano() > live {
		// 	log.Error("client %s message ID %d message expired.", client.ClientID, messageID)
		// 	model.RemoveOfflineMessage(client.ClientID, messageID)
		// 	continue
		// }

		if subChannel != message.ChannelID {
			continue
		}

		msg := &Message{
			Id:        util.Guid(message.ID).Hex(),
			Body:      []byte(message.Body),
			Timestamp: message.CreatedAt,
		}
		client.clientMsgChan <- msg
		model.RemoveOfflineMessage(client.ClientID, messageID)
	}
}

// hearbeat
func (p *protocol) HEARTBEAT(client *client, params [][]byte) ([]byte, error) {
	// log.Debug("[%s] heartbeat received", client)
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
	log.Debug("receive params on sub  %s", params)

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
	if err != nil {
		return nil, util.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
	}

	client_id, _ := strconv.ParseInt(string(params[1]), 10, 64)
	channel_id, _ := strconv.ParseInt(string(params[2]), 10, 64)
	message_id, _ := strconv.ParseInt(string(params[3]), 10, 64)

	destClient, err := p.context.broker.GetClient(client_id, channel_id)
	if err != nil || destClient == nil {
		err = p.ackPublish(client, util.ACK_OFF, client_id, message_id)
		log.Debug("client %s is null", client_id)
		return nil, err
	}
	// log.Debug("get client %s by channel %s = %s  ", client_id, channel_id, destClient)

	msg := &Message{
		Id:        util.Guid(message_id).Hex(),
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
	destClient.clientMsgChan <- msg
	err = p.ackPublish(client, util.ACT_ERR, client_id, message_id)
	return nil, err
}

func (p *protocol) ackPublish(client *client, ackType int32, clientID int64, msgID int64) (err error) {
	response := []byte(fmt.Sprintf("%d %d %d", ackType, clientID, msgID))
	err = p.Send(client, util.FrameTypeAck, response)
	if err != nil {
		log.Error("send response to worker_error %s ", err)
		return util.NewFatalClientErr(err, "E_OP", "SEND ACK failed")
	}
	return err
}

func readLen(r io.Reader, tmp []byte) (int32, error) {
	_, err := io.ReadFull(r, tmp)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(tmp)), nil
}
