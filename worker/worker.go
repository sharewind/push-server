package worker

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"log"
	"net"
	// "os"
	// "strings"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"code.sohuno.com/kzapp/push-server/broker"
	"code.sohuno.com/kzapp/push-server/client"
	"code.sohuno.com/kzapp/push-server/model"
	"code.sohuno.com/kzapp/push-server/util"
	// "github.com/bitly/go-simplejson"
)

// returned from ConnectToNSQ when already connected
var ErrAlreadyConnected = errors.New("already connected")

// returned from updateRdy if over max-in-flight
var ErrOverMaxInFlight = errors.New("over configure max-inflight")

// returned from ConnectToLookupd when given lookupd address exists already
var ErrLookupdAddressExists = errors.New("lookupd address already exists")

// Writer is a high-level type to publish to NSQ.
//
// A Writer instance is 1:1 with a destination `nsqd`
// and will lazily connect to that instance (and re-connect)
// when Publish commands are executed.

type Worker struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	// clientIDSequence int64

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

	// incomingMessages chan *Message

	lookupdRecheckChan chan int
	lookupdHTTPAddrs   []string
	lookupdQueryIndex  int

	runningHandlers int32
	stopFlag        int32
	stopHandler     sync.Once

	idChan          chan int64
	messageCount    uint64
	incomingMsgChan chan *model.Message
	// idChan     chan nsq.MessageID
	notifyChan chan interface{}
	exitChan   chan int
	waitGroup  util.WaitGroupWrapper
}

// returned when a publish command is made against a Writer that is not connected
var ErrNotConnected = errors.New("not connected")

// returned when a publish command is made against a Writer that has been stopped
var ErrStopped = errors.New("stopped")

// NewWriter returns an instance of Writer for the specified address
func NewWorker(HTTPAddress string) *Worker {
	// hostname, err := os.Hostname()
	// if err != nil {
	// 	log.Fatalf("ERROR: unable to get hostname %s", err.Error())
	// }

	httpAddr, err := net.ResolveTCPAddr("tcp", HTTPAddress)
	if err != nil {
		log.Fatal(err)
	}

	w := &Worker{
		httpAddr: httpAddr,
		// transactionChan: make(chan *WriterTransaction),
		exitChan: make(chan int),
		// closeChan: make(chan int),
		// dataChan:  make(chan []byte),

		// MaxAttemptCount: 5,

		// LookupdPollInterval: 60 * time.Second,
		// LookupdPollJitter:   0.3,

		// LowRdyIdleTimeout: 10 * time.Second,

		// DefaultRequeueDelay: 90 * time.Second,
		// MaxRequeueDelay:     15 * time.Minute,
		// BackoffMultiplier:   time.Second,

		// ShortIdentifier: strings.Split(hostname, ".")[0],
		// LongIdentifier:  hostname,

		ReadTimeout:       client.DefaultClientTimeout,
		WriteTimeout:      time.Second,
		HeartbeatInterval: time.Second / 2,

		DeflateLevel: 6,

		incomingMsgChan: make(chan *model.Message),

		pendingConnections: make(map[string]bool),
		nsqConnections:     make(map[string]*nsqConn),

		idChan: make(chan int64, 4096),
		// lookupdRecheckChan: make(chan int, 1), // used at connection close to force a possible reconnect
		// maxInFlight:        1,
		// backoffChan:        make(chan bool),
		// rdyChan:            make(chan *nsqConn, 1),

	}

	w.waitGroup.Wrap(func() { w.idPump() })
	w.waitGroup.Wrap(func() { w.Publish() })
	return w
}

func (w *Worker) Main() {
	context := &context{w}

	// n.waitGroup.Wrap(func() { n.lookupLoop() })

	// tcpListener, err := net.Listen("tcp", n.tcpAddr.String())
	// if err != nil {
	// 	log.Fatalf("FATAL: listen (%s) failed - %s", n.tcpAddr, err.Error())
	// }
	// n.tcpListener = tcpListener
	// tcpServer := &tcpServer{context: context}
	// n.waitGroup.Wrap(func() { util.TCPServer(n.tcpListener, tcpServer) })

	httpListener, err := net.Listen("tcp", w.httpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", w.httpAddr, err.Error())
	}
	w.httpListener = httpListener
	httpServer := &httpServer{context: context}
	w.waitGroup.Wrap(func() { util.HTTPServer(w.httpListener, httpServer) })

	// if n.options.StatsdAddress != "" {
	// 	n.waitGroup.Wrap(func() { n.statsdLoop() })
	// }
}

// PutMessage writes to the appropriate incoming message channel
func (w *Worker) PutMessage(msg *model.Message) error {
	w.RLock()
	defer w.RUnlock()
	// if atomic.LoadInt32(&t.exitFlag) == 1 {
	// 	return errors.New("exiting")
	// }
	w.incomingMsgChan <- msg
	atomic.AddUint64(&w.messageCount, 1)
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

func (w *Worker) Publish() {

	for {
		select {
		case message := <-w.incomingMsgChan:
			// save message on mongodb
			channel_id := message.ChannelID
			// devices_ids := querySubscibeDevices(channel_id)
			devices_ids := []int64{int64(185980656)}
			for _, device_id := range devices_ids {
				log.Printf("prepare send message channel_id %d device_id %d  body %s", channel_id, device_id, message.Body)
				// broker_id,err := getBrokerForDevice(device_id)
				// if err != nil || broker_id == nil{
				// 	saveOfflineMessage(device_id, message_id)
				// }

				broker_addr := "localhost:8600"
				conn := w.nsqConnections[broker_addr]
				log.Printf(" publish conn is %s", conn)
				cmd := client.Publish(device_id, int64(1001), message.ID, []byte(message.Body))

				var buf bytes.Buffer
				err := conn.sendCommand(&buf, cmd)
				if err != nil {
					log.Printf("error occuar on send Command")
					// saveOfflineMessage
					// increase failure count
				}
				log.Printf(" send message success = %s", message.Body)
			}
		case <-w.exitChan:
			goto exit

		}
	}
exit:
	log.Printf("publish msg exit!")
}

func (n *Worker) idPump() {
	factory := &util.GuidFactory{}
	lastError := time.Now()
	WorkerID := int64(2) //TODO
	for {
		id, err := factory.NewGUID(WorkerID)
		if err != nil {
			now := time.Now()
			if now.Sub(lastError) > time.Second {
				// only print the error once/second
				log.Printf("ERROR: %s", err.Error())
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
	log.Printf("ID: closing")
}

// ConnectToNSQ takes a nsqd address to connect directly to.
//
// It is recommended to use ConnectToLookupd so that topics are discovered
// automatically.  This method is useful when you want to connect to a single, local,
// instance.
func (q *Worker) ConnectToBroker(addr string) error {
	log.Printf("connect to broker %s", addr)

	var buf bytes.Buffer
	// if atomic.LoadInt32(&q.stopFlag) == 1 {
	// 	return errors.New("reader stopped")
	// }

	// if atomic.LoadInt32(&q.runningHandlers) == 0 {
	// 	return errors.New("no handlers")
	// }

	q.RLock()
	_, ok := q.nsqConnections[addr]
	_, pendingOk := q.pendingConnections[addr]
	if ok || pendingOk {
		q.RUnlock()
		return ErrAlreadyConnected
	}
	q.RUnlock()

	log.Printf("[%s] connecting to nsqd", addr)

	connection, err := newNSQConn(nil, addr, q.ReadTimeout, q.WriteTimeout)
	if err != nil {
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
	ci["sample_rate"] = q.SampleRate
	ci["user_agent"] = userAgent
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

		log.Printf("[%s] IDENTIFY response: %+v", connection, resp)

		// connection.maxRdyCount = resp.MaxRdyCount
		// if resp.MaxRdyCount < int64(q.MaxInFlight()) {
		// 	log.Printf("[%s] max RDY count %d < reader max in flight %d, truncation possible",
		// 		connection, resp.MaxRdyCount, q.MaxInFlight())
		// }

		// if resp.TLSv1 {
		// 	log.Printf("[%s] upgrading to TLS", connection)
		// 	err := connection.upgradeTLS(q.TLSConfig)
		// 	if err != nil {
		// 		cleanupConnection()
		// 		return fmt.Errorf("[%s] error (%s) upgrading to TLS", connection, err.Error())
		// 	}
		// }

		// if resp.Deflate {
		// 	log.Printf("[%s] upgrading to Deflate", connection)
		// 	err := connection.upgradeDeflate(q.DeflateLevel)
		// 	if err != nil {
		// 		connection.Close()
		// 		return fmt.Errorf("[%s] error (%s) upgrading to deflate", connection, err.Error())
		// 	}
		// }

		// if resp.Snappy {
		// 	log.Printf("[%s] upgrading to Snappy", connection)
		// 	err := connection.upgradeSnappy()
		// 	if err != nil {
		// 		connection.Close()
		// 		return fmt.Errorf("[%s] error (%s) upgrading to snappy", connection, err.Error())
		// 	}
		// }
	}

	// cmd = Subscribe(q.TopicName, q.ChannelName)
	// err = connection.sendCommand(&buf, cmd)
	// if err != nil {
	// 	cleanupConnection()
	// 	return fmt.Errorf("[%s] failed to subscribe to %s:%s - %s", connection, q.TopicName, q.ChannelName, err.Error())
	// }

	connection.enableReadBuffering()

	q.Lock()
	delete(q.pendingConnections, addr)
	q.nsqConnections[connection.String()] = connection
	q.Unlock()

	// pre-emptive signal to existing connections to lower their RDY count
	// q.RLock()
	// for _, c := range q.nsqConnections {
	// 	c.rdyChan <- c
	// }
	// q.RUnlock()

	log.Printf("connect to broker %s success", addr)
	connection.wg.Add(2)
	go q.readLoop(connection)
	go q.messagePump(connection)
	// go q.finishLoop(connection)

	return nil
}

// func handleError(q *Reader, c *nsqConn, errMsg string) {
// 	log.Printf(errMsg)
// 	atomic.StoreInt32(&c.stopFlag, 1)

// 	q.RLock()
// 	numLookupd := len(q.lookupdHTTPAddrs)
// 	q.RUnlock()
// 	if numLookupd == 0 {
// 		go func(addr string) {
// 			for {
// 				log.Printf("[%s] re-connecting in 15 seconds...", addr)
// 				time.Sleep(15 * time.Second)
// 				if atomic.LoadInt32(&q.stopFlag) == 1 {
// 					break
// 				}
// 				err := q.ConnectToNSQ(addr)
// 				if err != nil && err != ErrAlreadyConnected {
// 					log.Printf("ERROR: failed to connect to %s - %s",
// 						addr, err.Error())
// 					continue
// 				}
// 				break
// 			}
// 		}(c.RemoteAddr().String())
// 	}
// }

func (c *Worker) messagePump(conn *nsqConn) {
	var err error
	var buf bytes.Buffer

	heartbeatTicker := time.NewTicker(c.HeartbeatInterval)
	heartbeatChan := heartbeatTicker.C

	for {
		select {
		case <-heartbeatChan:
			cmd := client.HeartBeat()
			err = conn.sendCommand(&buf, cmd)
			if err != nil {
				log.Printf("ERROR: [%s] failed to write HeartBeat - %s", c, err)
				goto exit
			}
			// shoud receive response
			// conn.SetReadDeadline(time.Now().Add(c.HeartbeatInterval * 2))
		case <-c.exitChan:
			goto exit
		}
	}

exit:
	log.Printf("client: [%s] exiting messagePump", c)
	heartbeatTicker.Stop()
	if err != nil {
		log.Printf("client: [%s] messagePump error - %s", c, err.Error())
	}
}

func (c *Worker) readLoop(conn *nsqConn) {
	rbuf := bufio.NewReader(conn)
	for {
		// if atomic.LoadInt32(&c.stopFlag) == 1 {
		// 	goto exit
		// }

		resp, err := client.ReadResponse(rbuf)
		frameType, data, err := client.UnpackResponse(resp)

		if err != nil {
			//handleError(q, c, fmt.Sprintf("[%s] error (%s) reading response %d %s", c, err.Error(), frameType, data))
			continue
		}

		switch frameType {
		case client.FrameTypeMessage:
			msg, err := broker.DecodeMessage(data)
			// msg.cmdChan = c.cmdChan
			// msg.responseChan = c.finishedMessages

			if err != nil {
				// handleError(q, c, fmt.Sprintf("[%s] error (%s) decoding message %s",
				// 	c, err.Error(), data))
				continue
			}
			log.Printf("msg receive %s", msg)

			// remain := atomic.AddInt64(&c.rdyCount, -1)
			// atomic.AddInt64(&q.totalRdyCount, -1)
			// atomic.AddUint64(&c.messagesReceived, 1)
			// atomic.AddUint64(&q.MessagesReceived, 1)
			// atomic.AddInt64(&c.messagesInFlight, 1)
			// atomic.AddInt64(&q.messagesInFlight, 1)
			// atomic.StoreInt64(&c.lastMsgTimestamp, time.Now().UnixNano())

			// if q.VerboseLogging {
			// 	log.Printf("[%s] (remain %d) FrameTypeMessage: %s - %s",
			// 		c, remain, msg.Id, msg.Body)
			// }

			// q.incomingMessages <- msg
			// c.rdyChan <- c
		case client.FrameTypeResponse:
			switch {
			case bytes.Equal(data, []byte("CLOSE_WAIT")):
				// server is ready for us to close (it ack'd our StartClose)
				// we can assume we will not receive any more messages over this channel
				// (but we can still write back responses)
				log.Printf("[%s] received ACK from nsqd - now in CLOSE_WAIT", c)
				atomic.StoreInt32(&c.stopFlag, 1)
			case bytes.Equal(data, []byte("HT")):
				// var buf bytes.Buffer
				log.Printf("[%s] heartbeat received", conn)
				// err := c.sendCommand(&buf, Nop())
				// if err != nil {
				// 	handleError(q, c, fmt.Sprintf("[%s] error sending NOP - %s",
				// 		c, err.Error()))
				// 	goto exit
				// }
			}
		case client.FrameTypeError:
			log.Printf("[%s] error from nsqd %s", c, data)
		default:
			log.Printf("[%s] unknown message type %d", c, frameType)
		}
	}

	// exit:
	// c.wg.Done()
	log.Printf("[%s] readLoop exiting", c)
}

// Stop will gracefully stop the Reader
// func (q *Worker) Stop() {
// 	var buf bytes.Buffer

// 	if !atomic.CompareAndSwapInt32(&q.stopFlag, 0, 1) {
// 		return
// 	}

// 	log.Printf("stopping reader")

// 	q.RLock()
// 	l := len(q.nsqConnections)
// 	q.RUnlock()

// 	if l == 0 {
// 		q.stopHandlers()
// 	} else {
// 		q.RLock()
// 		for _, c := range q.nsqConnections {
// 			err := c.sendCommand(&buf, StartClose())
// 			if err != nil {
// 				log.Printf("[%s] failed to start close - %s", c, err.Error())
// 			}
// 		}
// 		q.RUnlock()

// 		time.AfterFunc(time.Second*30, func() {
// 			q.stopHandlers()
// 		})
// 	}
// }
