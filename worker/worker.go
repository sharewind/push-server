package worker

import (
	// "bufio"
	"bytes"
	"crypto/md5"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/op/go-logging"
	"hash/crc32"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"code.sohuno.com/kzapp/push-server/broker"
	"code.sohuno.com/kzapp/push-server/client"
	"code.sohuno.com/kzapp/push-server/model"
	. "code.sohuno.com/kzapp/push-server/util"

	// "github.com/bitly/go-simplejson"
)

var log = logging.MustGetLogger("worker")

var separatorBytes = []byte(" ")

// returned from ConnectToNSQ when already connected
var ErrAlreadyConnected = errors.New("already connected")

// returned from updateRdy if over max-in-flight
var ErrOverMaxInFlight = errors.New("over configure max-inflight")

// returned from ConnectToLookupd when given lookupd address exists already
var ErrLookupdAddressExists = errors.New("lookupd address already exists")

type PubMessage struct {
	DeviceID int64
	Message  *model.Message
}

type AckMessage struct {
	DeviceID  int64
	MessageID int64
	AckType   int32
}

type Worker struct {
	sync.RWMutex
	VerboseLogging bool // enable verbose logging
	// options *nsqdOptions

	// network deadlines
	ReadTimeout       time.Duration // the deadline set for network reads
	WriteTimeout      time.Duration // the deadline set for network writes
	HeartbeatInterval time.Duration

	// transport layer security
	TLSv1     bool        // negotiate enabling TLS
	TLSConfig *tls.Config // client TLS configuration

	// compression
	Deflate      bool // negotiate enabling Deflate compression
	DeflateLevel int  // the compression level to negotiate for Deflate
	Snappy       bool // negotiate enabling Snappy compression

	SampleRate int32  // set the sampleRate of the client's messagePump (requires nsqd 0.2.25+)
	UserAgent  string // a string identifying the agent for this client in the spirit of HTTP (default: "<client_library_name>/<version>")

	// topicMap map[string]*Topic

	// lookupPeers []*lookupPeer

	// tcpAddr      *net.TCPAddr
	httpAddr *net.TCPAddr
	// tcpListener  net.Listener
	httpListener net.Listener
	// tlsConfig    *tls.Config

	pendingConnections map[string]bool
	nsqConnections     map[string]*nsqConn

	stopFlag    int32
	stopHandler sync.Once

	idChan            chan int64
	incomingMsgChan   chan *model.Message
	clientPubChan     chan *PubMessage
	clientOfflineChan chan *PubMessage
	ackChan           chan *AckMessage
	exitChan          chan int
	waitGroup         WaitGroupWrapper
	// notifyChan      chan interface{}

	PubCount      uint64
	MessageCount  uint64
	FinishedCount uint64
	ErrorCount    uint64
}

// returned when a publish command is made against a Writer that is not connected
var ErrNotConnected = errors.New("not connected")

// returned when a publish command is made against a Writer that has been stopped
var ErrStopped = errors.New("stopped")

// NewWriter returns an instance of Writer for the specified address
func NewWorker(options *workerOptions) *Worker {
	// hostname, err := os.Hostname()
	// if err != nil {
	// 	log.Fatalf("ERROR: unable to get hostname %s", err.Error())
	// }

	httpAddr, err := net.ResolveTCPAddr("tcp", options.HTTPAddress)
	if err != nil {
		log.Fatal(err)
	}

	w := &Worker{
		httpAddr: httpAddr,
		exitChan: make(chan int),

		ReadTimeout:       DefaultClientTimeout,
		WriteTimeout:      time.Second,
		HeartbeatInterval: 30 * time.Second,

		DeflateLevel: 6,

		incomingMsgChan:   make(chan *model.Message),
		clientPubChan:     make(chan *PubMessage, 1024000),
		clientOfflineChan: make(chan *PubMessage, 1024000),
		ackChan:           make(chan *AckMessage, 1024000),

		pendingConnections: make(map[string]bool),
		nsqConnections:     make(map[string]*nsqConn),

		idChan: make(chan int64, 4096),
	}

	w.waitGroup.Wrap(func() { w.idPump() })
	// w.waitGroup.Wrap(func() { w.Publish() })

	return w
}

func (w *Worker) Main() {
	context := &context{w}

	for i := 0; i < 160; i++ {
		w.waitGroup.Wrap(func() { w.router() })
	}

	httpListener, err := net.Listen("tcp", w.httpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", w.httpAddr, err.Error())
	}
	w.httpListener = httpListener
	httpServer := &httpServer{context: context}
	w.waitGroup.Wrap(func() { HTTPServer(w.httpListener, httpServer) })
}

// PutMessage writes to the appropriate incoming message channel
func (w *Worker) PutMessage(msg *model.Message) error {
	w.RLock()
	defer w.RUnlock()
	// if atomic.LoadInt32(&t.exitFlag) == 1 {
	// 	return errors.New("exiting")
	// }
	w.incomingMsgChan <- msg
	atomic.AddUint64(&w.PubCount, 1)
	log.Debug("[worker]<PutMessage> %#v", msg)
	return nil
}

// Stop disconnects and permanently stops the Writer
// func (c *Client) Stop() {
// 	if !atomic.CompareAndSwapInt32(&c.stopFlag, 0, 1) {
// 		return
// 	}
// 	c.Close()
// 	c.wg.Wait()
// }

func (w *Worker) pushMessage(message *model.Message) {
	log.Debug("[worker]<pushMessage> %#v", message)
	// save message on mongodb
	// devices_ids := querySubscibeDevices(channel_id)
	// skip := 0
	limit := 1000
	// total, err := model.CountSubscribeByChannelId(message.ChannelID, message.DeviceType)
	// if err != nil {
	// 	log.Error(err.Error())
	// }
	// pageCount := (((total - 1) / 1000) + 1)
	// log.Debug("worker all sum:%d time:%d", total, pageCount)

	lastID := int64(0)
	for i := 0; ; i++ {
		subs, err := model.FindSubscribeByChannelID(lastID, message.ChannelID, message.DeviceType, limit)
		if err != nil {
			log.Debug("ERROR: FindSubscribeByChannelID channelId=%d,deviceType=%d error=%s", message.ChannelID, message.DeviceType, err)
			return
		}

		if subs == nil || len(subs) == 0 {
			break
		}

		for _, sub := range subs {
			atomic.AddUint64(&w.MessageCount, 1)
			w.clientPubChan <- &PubMessage{DeviceID: sub.DeviceID, Message: message}
			lastID = sub.ID
		}
		log.Debug("get subs page %d  count %d  lastID %d finished!", i, len(subs), lastID)
	}
	log.Debug("get subs_all finished!.........")
}

func (w *Worker) router() {
	log.Debug("router start ...........")
	for {
		select {
		case message := <-w.incomingMsgChan:
			log.Debug("get imcoming %s", message)
			w.pushMessage(message)
		case pub := <-w.clientPubChan:
			// log.Debug("get pub  %s", pub)
			//TODO save pub msg to mongo on stop
			w.sendMessage2Client(pub)
			// FIXME should process err on send
		case pub := <-w.clientOfflineChan:
			// log.Debug("get off %s", pub)
			//TODO save offline msg to mongo on stop
			model.SaveOfflineMessage(pub.DeviceID, pub.Message.ID)
		case ack := <-w.ackChan:
			log.Debug("get ack %s", ack)
			w.processAck(ack)
		case <-w.exitChan:
			goto exit

		}
	}
exit:
	log.Debug("msg router exit!")
}

func (w *Worker) processAck(ack *AckMessage) {
	log.Debug("process ack  %s", ack)
	if ack.AckType != ACK_SUCCESS {
		model.SaveOfflineMessage(ack.DeviceID, ack.MessageID)
		model.IncrMsgErrCount(ack.MessageID, 1)
		model.IncrClientErrCount(ack.DeviceID, 1)
	} else {
		model.IncrMsgOKCount(ack.MessageID, 1)
		model.IncrClientOKCount(ack.DeviceID, 1)
	}
}

func (w *Worker) sendMessage2Client(pub *PubMessage) (err error) {

	broker_addr, err := model.GetClientConn(pub.DeviceID)
	if err != nil {
		// log.Debug("ERROR: GetClientConn by redis  [%d]  err %s ", pub.DeviceID, err)
		return errors.New("client offline")
	}
	// log.Debug("GetClientConn by redis  [%d]   %s ", pub.DeviceID, broker_addr)

	conn, ok := w.GetBroker(broker_addr)
	// log.Debug(" publish conn %s is %s", broker_addr, conn)
	if !ok {
		log.Debug("ERROR: Get nsqConnections  [%s]  err %s ", broker_addr, err)
		return errors.New("client offline")
	}

	message := pub.Message
	cmd := client.Publish(pub.DeviceID, message.ChannelID, message.ID, []byte(message.Body))
	conn.cmdChan <- cmd
	// log.Debug("send message success: channel_id %s, device_id %d,  body %s", message.ChannelID, pub.DeviceID, message.Body)
	return nil
}

func (n *Worker) idPump() {
	hostname, err := os.Hostname()
	//hostname := "sohu"
	if err != nil {
		log.Fatal(err)
	}

	h := md5.New()
	io.WriteString(h, hostname)
	ID := int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)

	factory := &GuidFactory{}
	lastError := time.Now()
	WorkerID := int64(ID) //TODO
	for {
		id, err := factory.NewGUID(WorkerID)
		if err != nil {
			now := time.Now()
			if now.Sub(lastError) > time.Second {
				// only print the error once/second
				log.Debug("ERROR: %s", err.Error())
				lastError = now
			}
			runtime.Gosched()
			continue
		}
		select {
		case n.idChan <- id:
		case <-n.exitChan:
			goto exit
		}
	}

exit:
	log.Debug("ID: closing")
}

func (w *Worker) GetBroker(broker_addr string) (conn *nsqConn, ok bool) {
	w.RLock()
	defer w.RUnlock()
	// log.Debug("GetBroker conns = %s,  broker= %s", w.nsqConnections, broker_addr)
	conn, ok = w.nsqConnections[broker_addr]
	return conn, ok
}

func (w *Worker) SafeConnectToBroker(addr []string) {
	for i := 0; i < len(addr); i++ {
		go func(addr string) {
			for {
				if atomic.LoadInt32(&w.stopFlag) == 1 {
					break
				}
				err := w.ConnectToBroker(addr)
				if err == nil {
					log.Info("connect to %s success ", addr)
					break
				}

				if err != nil && err != ErrAlreadyConnected {
					log.Debug("ERROR: failed to connect to %s - %s", addr, err.Error())
				}

				log.Debug("[%s] re-connecting in 15 seconds...", addr)
				time.Sleep(15 * time.Second)
			}
		}(addr[i])
	}
}

func (q *Worker) ConnectToBroker(addr string) error {
	log.Debug("connect to broker %s", addr)

	var buf bytes.Buffer
	if atomic.LoadInt32(&q.stopFlag) == 1 {
		return errors.New("worker stopped")
	}

	q.RLock()
	_, ok := q.nsqConnections[addr]
	_, pendingOk := q.pendingConnections[addr]
	if ok || pendingOk {
		log.Debug("[%s] conntectd already", addr)
		q.RUnlock()
		return ErrAlreadyConnected
	}
	q.RUnlock()

	log.Debug("[%s] connecting to broker", addr)

	connection, err := newNSQConn(nil, addr, q.ReadTimeout, q.WriteTimeout)
	if err != nil {
		log.Warning("failure to connected to [%s]", addr)
		return err
	}
	cleanupConnection := func() {
		q.Lock()
		delete(q.pendingConnections, addr)
		q.Unlock()
		connection.Close()
	}
	q.pendingConnections[addr] = true

	// set the user_agent string to the default if there is no user input version
	VERSION := 1
	userAgent := fmt.Sprintf("go-nsq/%s", VERSION)
	if q.UserAgent != "" {
		userAgent = q.UserAgent
	}

	ci := make(map[string]interface{})
	// ci["short_id"] = q.ShortIdentifier
	// ci["long_id"] = q.LongIdentifier
	ci["tls_v1"] = q.TLSv1
	ci["deflate"] = q.Deflate
	ci["deflate_level"] = q.DeflateLevel
	ci["snappy"] = q.Snappy
	ci["feature_negotiation"] = true
	ci["user_agent"] = userAgent
	ci["role"] = "$_@push_sign_$_kz_worker"
	cmd, err := client.Identify(ci)
	if err != nil {
		cleanupConnection()
		return fmt.Errorf("[%s] failed to create identify command - %s", connection, err.Error())
	}

	err = connection.sendCommand(&buf, cmd)
	if err != nil {
		cleanupConnection()
		return fmt.Errorf("[%s] failed to identify - %s", connection, err.Error())
	}

	_, data, err := connection.readUnpackedResponse()
	if err != nil {
		cleanupConnection()
		return fmt.Errorf("[%s] error reading response %s", connection, err.Error())
	}

	// check to see if the server was able to respond w/ capabilities
	if data[0] == '{' {
		resp := struct {
			MaxRdyCount int64 `json:"max_rdy_count"`
			TLSv1       bool  `json:"tls_v1"`
			Deflate     bool  `json:"deflate"`
			Snappy      bool  `json:"snappy"`
			SampleRate  int32 `json:"sample_rate"`
		}{}
		err := json.Unmarshal(data, &resp)
		if err != nil {
			cleanupConnection()
			return fmt.Errorf("[%s] error (%s) unmarshaling IDENTIFY response %s", connection, err.Error(), data)
		}

		log.Debug("[%s] IDENTIFY response: %+v", connection, resp)

		// connection.maxRdyCount = resp.MaxRdyCount
		// if resp.MaxRdyCount < int64(q.MaxInFlight()) {
		// 	log.Debug("[%s] max RDY count %d < reader max in flight %d, truncation possible",
		// 		connection, resp.MaxRdyCount, q.MaxInFlight())
		// }

		if resp.TLSv1 {
			log.Debug("[%s] upgrading to TLS", connection)
			err := connection.upgradeTLS(q.TLSConfig)
			if err != nil {
				cleanupConnection()
				return fmt.Errorf("[%s] error (%s) upgrading to TLS", connection, err.Error())
			}
		}

		if resp.Deflate {
			log.Debug("[%s] upgrading to Deflate", connection)
			err := connection.upgradeDeflate(q.DeflateLevel)
			if err != nil {
				connection.Close()
				return fmt.Errorf("[%s] error (%s) upgrading to deflate", connection, err.Error())
			}
		}

		if resp.Snappy {
			log.Debug("[%s] upgrading to Snappy", connection)
			err := connection.upgradeSnappy()
			if err != nil {
				connection.Close()
				return fmt.Errorf("[%s] error (%s) upgrading to snappy", connection, err.Error())
			}
		}
	}
	connection.enableReadBuffering()

	q.Lock()
	delete(q.pendingConnections, addr)
	q.nsqConnections[connection.String()] = connection
	q.Unlock()

	log.Debug("connect to broker %s success", addr)
	connection.wg.Add(2)
	go q.readLoop(connection)
	go q.messagePump(connection)

	return nil
}

func handleError(w *Worker, c *nsqConn, errMsg string) {
	log.Debug("[%s] handleError %s", c, errMsg)
	atomic.StoreInt32(&c.stopFlag, 1)

	go func(addr string) {
		for {
			log.Debug("[%s] re-connecting in 15 seconds...", addr)
			time.Sleep(15 * time.Second)
			if atomic.LoadInt32(&w.stopFlag) == 1 {
				break
			}
			err := w.ConnectToBroker(addr)
			if err != nil && err != ErrAlreadyConnected {
				log.Debug("ERROR: failed to connect to %s - %s",
					addr, err.Error())
				continue
			}
			break
		}
	}(c.RemoteAddr().String())
	// }
}

func (w *Worker) messagePump(c *nsqConn) {
	var err error
	var buf bytes.Buffer

	heartbeatTicker := time.NewTicker(w.HeartbeatInterval)
	heartbeatChan := heartbeatTicker.C

	for {
		if atomic.LoadInt32(&c.stopFlag) == 1 || atomic.LoadInt32(&w.stopFlag) == 1 {
			goto exit
		}

		select {
		case <-c.exitChan:
			log.Debug("[%s] breaking out of message pump loop", c)
			// Indicate drainReady because we will not pull any more off finishedMessages
			// close(c.drainReady)
			goto exit
		case cmd := <-c.cmdChan:
			err := c.sendCommand(&buf, cmd)
			if err != nil {
				atomic.AddUint64(&w.ErrorCount, 1)
				log.Debug("[%s] error sending command %s - %s", c, cmd, err)
				w.stopBrokerConn(c)
				log.Debug("[%s] trigger stopBrokerConn on sendCommand err %s", c, err)
				continue
			}
			atomic.AddUint64(&w.FinishedCount, 1)
		case <-heartbeatChan:
			cmd := client.HeartBeat()
			err = c.sendCommand(&buf, cmd)
			if err != nil {
				log.Debug("ERROR: [%s] failed to writing heartbeat - %s", c, err)
				w.stopBrokerConn(c)
				log.Debug("[%s] trigger stopBrokerConn on sendCommand err %s", c, err)
				continue
			}
			// shoud receive response
			// conn.SetReadDeadline(time.Now().Add(c.HeartbeatInterval * 2))
		case <-w.exitChan:
			goto exit
		}
	}

exit:
	heartbeatTicker.Stop()
	if err != nil {
		log.Debug("broker: [%s] messagePump error - %s", c, err.Error())
	}

	c.wg.Done()
	log.Debug("broker: [%s] exiting messagePump", c)
}

func (w *Worker) readLoop(c *nsqConn) {
	for {
		// log.Info("[%s] client readLoop", c)

		if atomic.LoadInt32(&w.stopFlag) == 1 || atomic.LoadInt32(&c.stopFlag) == 1 {
			log.Info("[%s] stopBrokerConn on client stopFlag ", c)
			w.stopBrokerConn(c)
			goto exit
		}

		frameType, data, err := c.readUnpackedResponse()
		// log.Info("[%s] readUnpackResponse %s ", c, data)
		if err != nil {
			handleError(w, c, fmt.Sprintf("[%s] error (%s) reading response %d %s", c, err.Error(), frameType, data))
			continue
		}

		switch frameType {
		case FrameTypeMessage:
			msg, err := broker.DecodeMessage(data)

			if err != nil {
				handleError(w, c, fmt.Sprintf("[%s] error (%s) reading response %d %s", c, err.Error(), frameType, data))
				continue
			}

			// if q.VerboseLogging {
			log.Info("[%s] FrameTypeMessage receive  %s - %s", c, msg.Id, msg.Body)
			// }

		case FrameTypeResponse:
			switch {
			case bytes.Equal(data, []byte("CLOSE_WAIT")):
				// server is ready for us to close (it ack'd our StartClose)
				// we can assume we will not receive any more messages over this channel
				// (but we can still write back responses)
				log.Debug("[%s] received ACK from nsqd - now in CLOSE_WAIT", c)
				atomic.StoreInt32(&c.stopFlag, 1)
			case bytes.Equal(data, []byte("H")):
				log.Debug("[%s] heartbeat received", c)
			}
		case FrameTypeAck:
			log.Debug("[%s] ack receive %s", c, data)
			params := bytes.Split(data, separatorBytes)
			ackType, err := strconv.ParseInt(string(params[0]), 10, 64)
			deviceId, err := strconv.ParseInt(string(params[1]), 10, 64)
			msgId, err := strconv.ParseInt(string(params[2]), 10, 64)
			if err != nil {
				log.Debug("ERROR: parse msgId error %s", err)
				continue
			}
			w.ackChan <- &AckMessage{deviceId, msgId, int32(ackType)}

		case FrameTypeError:
			log.Debug("[%s] error from nsqd %s", c, data)
		default:
			log.Debug("[%s] unknown message type %d", c, frameType)
		}
	}

exit:
	c.wg.Done()
	log.Debug("[%s] readLoop exiting", c)
}

func (q *Worker) stopBrokerConn(c *nsqConn) {
	c.stopper.Do(func() {
		log.Debug("[%s] beginning stopBrokerConn!", c)
		close(c.exitChan)
		c.Close()
		go q.cleanupConnection(c)
	})
}

func (q *Worker) cleanupConnection(c *nsqConn) {

	log.Debug("[%s] cleanupConnection", c)
	// this blocks until finishLoop and readLoop have exited
	c.wg.Wait()
	// log.Debug("clean up readLoop done conn[%s]", c)

	q.Lock()
	delete(q.nsqConnections, c.String())
	left := len(q.nsqConnections)
	q.Unlock()

	// log.Debug("clean up conn[%s] success!", c)
	log.Debug("there are %d connections left alive", left)
}

// Stop will gracefully stop the Reader
func (q *Worker) Stop() {
	var buf bytes.Buffer
	if !atomic.CompareAndSwapInt32(&q.stopFlag, 0, 1) {
		return
	}

	log.Debug("stopping worker")
	if q.httpListener != nil {
		q.httpListener.Close()
	}

	//TODO 此处需要持久化尚未发送的消息队列 incomingMsgChan 到数据库

	q.RLock()
	left := len(q.nsqConnections)
	q.RUnlock()

	if left > 0 {
		q.RLock()
		for _, c := range q.nsqConnections {
			err := c.sendCommand(&buf, client.StartClose())
			if err != nil {
				log.Debug("[%s] failed to start close - %s", c, err.Error())
			}
		}
		q.RUnlock()

		// TODO 是否等到所有的消息收到ACT
		// time.AfterFunc(time.Second*30, func() {
		// 	q.stopHandlers()
		// })
	}

	close(q.exitChan)
	q.waitGroup.Wait()
}

func (w *Worker) GetStats() string {
	result := fmt.Sprintf("[WorkerStatus]  MessageCount:%d, FinishedCount:%d, ErrorCount:%d ", atomic.LoadUint64(&w.MessageCount), atomic.LoadUint64(&w.FinishedCount), atomic.LoadUint64(&w.ErrorCount))
	return result
}

func (w *Worker) ResetStats() {
	atomic.StoreUint64(&w.MessageCount, 0)
	atomic.StoreUint64(&w.FinishedCount, 0)
	atomic.StoreUint64(&w.ErrorCount, 0)
}
