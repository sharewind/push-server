package broker

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"strconv"
	// "math/rand"
	"net"
	"sync/atomic"
	"time"
	// "unsafe"
	// "github.com/bitly/go-nsq"

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

	// clientID := atomic.AddInt64(&p.context.broker.clientIDSequence, 1)
	client := newClient(conn, p.context)

	// synchronize the startup of messagePump in order
	// to guarantee that it gets a chance to initialize
	// goroutine local state derived from client attributes
	// and avoid a potential race with IDENTIFY (where a client
	// could have changed or disabled said attributes)

	// messagePumpStartedChan := make(chan bool)
	// go p.messagePump(client, messagePumpStartedChan)
	// <-messagePumpStartedChan

	for {
		log.Printf("INFO: client[%s] HeartbeatInterval %d ", client, client.HeartbeatInterval)
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
			log.Printf("PROTOCOL(V2): [%s] %s", client, params)
		}

		response, err := p.Exec(client, params)
		if err != nil {
			context := ""
			if parentErr := err.(util.ChildErr).Parent(); parentErr != nil {
				context = " - " + parentErr.Error()
			}
			log.Printf("ERROR: [%s] - %s%s", client, err.Error(), context)

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
				log.Printf("ERROR: send response to client error %s ", err)
				break
			}
		}
	}

	log.Printf("PROTOCOL(V2): [%s] exiting ioloop", client)
	p.cleanupClientConn(client)
	return err
}

func (p *protocol) cleanupClientConn(client *client) {
	client.Close()
	model.DelClientConn(client.ClientID)
	p.context.broker.RemoveClient(client.ClientID, client.SubChannel)

	client_id, _ := strconv.ParseInt(client.ClientID, 10, 64)
	// touch devie online
	model.TouchDeviceOffline(client_id)

	close(client.ExitChan)
	// if client.Channel != nil {
	// 	client.Channel.RemoveClient(client.ID)
	// }
}

func (p *protocol) SendMessage(client *client, msg *Message, buf *bytes.Buffer) error {
	// if p.context.broker.options.Verbose {
	log.Printf("PROTOCOL(V2): writing msg(%s) to client(%s) - %s",
		msg.Id, client, msg.Body)
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

	log.Printf("PROTOCOL(V2): Success writing msg(%s) to client(%s) - %s",
		msg.Id, client, msg.Body)
	return nil
}

func (p *protocol) Send(client *client, frameType int32, data []byte) error {
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
	case bytes.Equal(params[0], []byte("HT")):
		return p.HT(client, params)
	case bytes.Equal(params[0], []byte("FIN")):
		return p.FIN(client, params)
	// case bytes.Equal(params[0], []byte("RDY")):
	// 	return p.RDY(client, params)
	// case bytes.Equal(params[0], []byte("REQ")):
	// 	return p.REQ(client, params)
	case bytes.Equal(params[0], []byte("PUB")):
		return p.PUB(client, params)
	// case bytes.Equal(params[0], []byte("MPUB")):
	// 	return p.MPUB(client, params)
	case bytes.Equal(params[0], []byte("NOP")):
		return p.NOP(client, params)
	// case bytes.Equal(params[0], []byte("TOUCH")):
	// 	return p.TOUCH(client, params)
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
		log.Printf("PROTOCOL(V2): [%s] %+v", client, identifyData)
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
		// MaxRdyCount     int64  `json:"max_rdy_count"`
		Version         string `json:"version"`
		MaxMsgTimeout   int64  `json:"max_msg_timeout"`
		MsgTimeout      int64  `json:"msg_timeout"`
		TLSv1           bool   `json:"tls_v1"`
		Deflate         bool   `json:"deflate"`
		DeflateLevel    int    `json:"deflate_level"`
		MaxDeflateLevel int    `json:"max_deflate_level"`
		Snappy          bool   `json:"snappy"`
		SampleRate      int32  `json:"sample_rate"`
	}{
		// MaxRdyCount:     p.context.broker.options.MaxRdyCount,
		Version:         util.BINARY_VERSION,
		MaxMsgTimeout:   int64(p.context.broker.options.MaxMsgTimeout / time.Millisecond),
		MsgTimeout:      int64(p.context.broker.options.MsgTimeout / time.Millisecond),
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
		log.Printf("PROTOCOL(V2): [%s] upgrading connection to TLS", client)
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
		log.Printf("PROTOCOL(V2): [%s] upgrading connection to snappy", client)
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
		log.Printf("PROTOCOL(V2): [%s] upgrading connection to deflate", client)
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

	log.Printf("receive params on sub  %s", params)
	if len(params) < 1 {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "SUB insufficient number of parameters")
	}

	//TODO FIXME
	str_channel_id := string(params[1])
	channel_id, err := strconv.ParseInt(str_channel_id, 10, 64) //TODO need validate channel_id
	if err != nil {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "invalid channel id ")
	}
	client_id, err := strconv.ParseInt(client.ClientID, 10, 64)
	if err != nil {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "invalid client id ")
	}

	channel, err := model.FindChannelByID(channel_id)
	if err != nil || channel == nil {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "invalid channel id ")
	}
	device, err := model.FindDeviceByID(client_id)
	if err != nil || device == nil {
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
		return nil, util.NewFatalClientErr(nil, "internal error", "save subscribe error")
	}

	p.context.broker.AddClient(client.ClientID, str_channel_id, client)
	log.Printf("INFO: clientId %d sub channel %s success ", client.ClientID, channel_id)

	// touch devie online
	model.TouchDeviceOnline(client_id)

	// should send client connected eventsf
	log.Printf("INFO: SetClientConn clientID=%s, broker_addr=%s", client.ClientID, client.LocalAddr().String())
	err = model.SetClientConn(client.ClientID, client.LocalAddr().String())
	if err != nil {
		return nil, util.NewFatalClientErr(nil, "internal error", "save subscribe error")
	}

	// increase channel sub count
	// add client to channel sub list

	// topic := p.context.broker.GetTopic(topicName)
	// channel := topic.GetChannel(channelName)
	// channel.AddClient(client.ID, client)

	atomic.StoreInt32(&client.State, StateSubscribed)
	client.SubChannel = str_channel_id
	go p.checkOfflineMessage(client)
	// client.Channel = channel
	// update message pump
	// client.SubEventChan <- channel

	return okBytes, nil
}

func (p *protocol) checkOfflineMessage(client *client) {
	var buf bytes.Buffer

	messageIDs, err := model.GetOfflineMessages(client.ClientID)
	if err != nil || messageIDs == nil {
		log.Printf("ERROR: GetOfflineMessages clientID %s error %s ", client.ClientID, err)
		return
	}

	subChannel, _ := strconv.ParseInt(client.SubChannel, 10, 64)
	for _, messageID := range messageIDs {
		msg, err := model.FindMessageByID(messageID)
		if err != nil || msg == nil {
			log.Printf("ERROR: client %s message ID %d message doesn't exist, err %s", client.ClientID, messageID, err)
			continue
		}

		// live := msg.CreatedAt + msg.Expires*100000000000000
		// if time.Now().UnixNano() > live {
		// 	log.Printf("ERROR: client %s message ID %d message expired.", client.ClientID, messageID)
		// 	model.RemoveOfflineMessage(client.ClientID, messageID)
		// 	continue
		// }

		if subChannel != msg.ChannelID {
			continue
		}

		msg2 := &Message{
			Id:        util.Guid(msg.ID).Hex(),
			Body:      []byte(msg.Body),
			Timestamp: msg.CreatedAt,
		}
		log.Printf("msg is %#v", msg2)
		err = p.SendMessage(client, msg2, &buf)
		if err != nil {
			log.Printf("send message to client %s error  %s", client, err)
		}

		client.Lock()
		err = client.Flush()
		client.Unlock()

		log.Printf("send message %#v to client %s success ", msg, client)
		model.RemoveOfflineMessage(client.ClientID, messageID)

	}
}

// hearbeat
func (p *protocol) HT(client *client, params [][]byte) ([]byte, error) {
	log.Printf("[%s] heartbeat received", client)
	return []byte("HT"), nil
}

func (p *protocol) FIN(client *client, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != StateSubscribed && state != StateClosing {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "cannot FIN in current state")
	}

	if len(params) < 2 {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "FIN insufficient number of params")
	}

	// id := *(*MessageID)(unsafe.Pointer(&params[1][0]))
	// err := client.Channel.FinishMessage(client.ID, id)
	// if err != nil {
	// 	return nil, util.NewClientErr(err, "E_FIN_FAILED",
	// 		fmt.Sprintf("FIN %s failed %s", id, err.Error()))
	// }

	// client.FinishedMessage()

	return nil, nil
}

func (p *protocol) CLS(client *client, params [][]byte) ([]byte, error) {
	// if atomic.LoadInt32(&client.State) != StateSubscribed {
	// 	return nil, util.NewFatalClientErr(nil, "E_INVALID", "cannot CLS in current state")
	// }

	client.StartClose()
	return []byte("CLOSE_WAIT"), nil
}

func (p *protocol) NOP(client *client, params [][]byte) ([]byte, error) {
	return nil, nil
}

func (p *protocol) PUB(client *client, params [][]byte) ([]byte, error) {
	// var err error
	var buf bytes.Buffer
	if client.Role != "$_@push_sign_$_kz_worker" {
		return nil, util.NewFatalClientErr(nil, "E_INVALID_REQUEST", "client can't pub message")
	}

	if len(params) < 3 {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "PUB insufficient number of parameters")
	}
	log.Printf("receive params on sub  %s", params)

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

	client_id := string(params[1])
	channel_id := string(params[2])
	message_id := string(params[3])
	msgId, _ := strconv.ParseInt(message_id, 10, 64)
	log.Printf("msgId ==  %d", msgId)

	// TODO 另外启动一个channel 与 goroutine 用来处理这个消息
	dstClient, err := p.context.broker.GetClient(client_id, channel_id)
	if err != nil || dstClient == nil {
		p.ackPublish(client, util.ACK_OFF, client_id, msgId)
		// model.SaveOfflineMessage(dstClient.ClientID, msgId)
		log.Printf("client %s is null", client_id)
		return nil, nil
		//return nil, util.NewFatalClientErr(nil, "E_INVALID", "PUB insufficient number of parameters")
		// 	return FrameTypeACKError message_id
	}
	log.Printf("get client %s by channel %s = %s  ", client_id, channel_id, dstClient)

	msg := &Message{
		Id:        util.Guid(msgId).Hex(),
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
	log.Printf("msg is %#v", msg)
	// dstClient.SendingMessage()
	err = p.SendMessage(dstClient, msg, &buf)
	if err != nil {
		log.Printf("send message to client %s error  %s", dstClient, err)
	}

	dstClient.Lock()
	err = dstClient.Flush()
	dstClient.Unlock()

	if err != nil {
		p.ackPublish(client, util.ACT_ERR, client_id, msgId)
	} else {
		p.ackPublish(client, util.ACK_SUCCESS, client_id, msgId)
	}
	return nil, nil
}

func (p *protocol) ackPublish(client *client, ackType int32, clientID string, msgID int64) (err error) {
	response := []byte(fmt.Sprintf("%d %s %d", ackType, clientID, msgID))
	err = p.Send(client, util.FrameTypeAck, response)
	if err != nil {
		log.Printf("ERROR: send response to client error %s ", err)
		p.cleanupClientConn(client)
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
