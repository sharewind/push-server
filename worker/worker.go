package worker

import (
    // "bufio"
    "bytes"
    "crypto/tls"
    "encoding/json"
    "errors"
    "fmt"
    "github.com/op/go-logging"
    "net"
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
    exitChan        chan int
    waitGroup       WaitGroupWrapper
    // notifyChan      chan interface{}
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

        ReadTimeout:       DefaultClientTimeout,
        WriteTimeout:      time.Second,
        HeartbeatInterval: 30 * time.Second,

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
    w.waitGroup.Wrap(func() { HTTPServer(w.httpListener, httpServer) })

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
            go w.pushMessage(message)
        case <-w.exitChan:
            goto exit

        }
    }
exit:
    log.Debug("publish msg exit!")
}

func (w *Worker) pushMessage(message *model.Message) {
    // save message on mongodb
    // devices_ids := querySubscibeDevices(channel_id)
    skip := 0
    limit := -1
    subs, err := model.FindSubscribeByChannelID(message.ChannelID, message.DeviceType, skip, limit)
    if err != nil {
        log.Debug("ERROR: FindSubscribeByChannelID channelId=%d,deviceType=%d error=%s", message.ChannelID, message.DeviceType, err)
        return
    }

    for _, sub := range subs {
        err := w.sendMessage2Client(&sub, message)
        if err != nil {
            log.Debug("INFO saveOfflineMessage clientID %d messageID %d", sub.DeviceID, message.ID)
            model.SaveOfflineMessage(sub.DeviceID, message.ID)
        }
    }
}

func (w *Worker) sendMessage2Client(sub *model.Subscribe, message *model.Message) (err error) {
    var buf bytes.Buffer
    log.Debug("prepare send message channel_id %d device_id %d  body %s", message.ChannelID, sub.DeviceID, message.Body)
    // broker_id,err := getBrokerForDevice(device_id)
    // if err != nil || broker_id == nil{
    // 	saveOfflineMessage(device_id, message_id)
    // }

    broker_addr, err := model.GetClientConn(sub.DeviceID)
    if err != nil {
        log.Debug("ERROR: GetClientConn by redis  [%s]  err %s ", sub.DeviceID, err)
        //TODO save offline message
        return errors.New("client offline")
    }

    // broker_addr := "localhost:8600"
    //TODO should be a func
    w.RLock()
    conn, ok := w.nsqConnections[broker_addr]
    w.RUnlock()

    log.Debug(" publish conn %s is %s", broker_addr, conn)
    if !ok {
        log.Debug("ERROR: Get nsqConnections  [%s]  err %s ", broker_addr, err)
        //TODO save offline message
        return errors.New("client offline")
    }

    cmd := client.Publish(sub.DeviceID, message.ChannelID, message.ID, []byte(message.Body))
    err = conn.sendCommand(&buf, cmd)
    if err != nil {
        log.Debug("ERROR: send to [%s] command %s err %s ", conn, cmd, err)
        // saveOfflineMessage
        // increase failure count
        w.stopBrokerConn(conn)
        return errors.New("client offline")
    }
    log.Debug(" send message success = %s", message.Body)
    return nil
}

func (n *Worker) idPump() {
    factory := &GuidFactory{}
    lastError := time.Now()
    WorkerID := int64(2) //TODO
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

// ConnectToNSQ takes a nsqd address to connect directly to.
//
// It is recommended to use ConnectToLookupd so that topics are discovered
// automatically.  This method is useful when you want to connect to a single, local,
// instance.
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
        q.RUnlock()
        return ErrAlreadyConnected
    }
    q.RUnlock()

    log.Debug("[%s] connecting to nsqd", addr)

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

        // if resp.TLSv1 {
        // 	log.Debug("[%s] upgrading to TLS", connection)
        // 	err := connection.upgradeTLS(q.TLSConfig)
        // 	if err != nil {
        // 		cleanupConnection()
        // 		return fmt.Errorf("[%s] error (%s) upgrading to TLS", connection, err.Error())
        // 	}
        // }

        // if resp.Deflate {
        // 	log.Debug("[%s] upgrading to Deflate", connection)
        // 	err := connection.upgradeDeflate(q.DeflateLevel)
        // 	if err != nil {
        // 		connection.Close()
        // 		return fmt.Errorf("[%s] error (%s) upgrading to deflate", connection, err.Error())
        // 	}
        // }

        // if resp.Snappy {
        // 	log.Debug("[%s] upgrading to Snappy", connection)
        // 	err := connection.upgradeSnappy()
        // 	if err != nil {
        // 		connection.Close()
        // 		return fmt.Errorf("[%s] error (%s) upgrading to snappy", connection, err.Error())
        // 	}
        // }
    }
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

    log.Debug("connect to broker %s success", addr)
    connection.wg.Add(2)
    go q.readLoop(connection)
    go q.messagePump(connection)
    // go q.finishLoop(connection)

    return nil
}

func handleError(q *Worker, c *nsqConn, errMsg string) {
    log.Debug(errMsg)
    atomic.StoreInt32(&c.stopFlag, 1)

    // q.RLock()
    // numLookupd := len(q.lookupdHTTPAddrs)
    // q.RUnlock()
    // if numLookupd == 0 {
    go func(addr string) {
        for {
            log.Debug("[%s] re-connecting in 15 seconds...", addr)
            time.Sleep(15 * time.Second)
            if atomic.LoadInt32(&q.stopFlag) == 1 {
                break
            }
            err := q.ConnectToBroker(addr)
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
        case <-heartbeatChan:
            cmd := client.HeartBeat()
            err = c.sendCommand(&buf, cmd)
            if err != nil {
                log.Debug("ERROR: [%s] failed to writing heartbeat - %s", c, err)
                w.stopBrokerConn(c)
                continue
            }
            // shoud receive response
            // conn.SetReadDeadline(time.Now().Add(c.HeartbeatInterval * 2))
        case <-w.exitChan:
            goto exit
        }
    }

exit:
    log.Debug("broker: [%s] exiting messagePump", c)
    heartbeatTicker.Stop()
    if err != nil {
        log.Debug("broker: [%s] messagePump error - %s", c, err.Error())
    }
}

func (w *Worker) readLoop(c *nsqConn) {
    for {
        if atomic.LoadInt32(&c.stopFlag) == 1 || atomic.LoadInt32(&c.stopFlag) == 1 {
            goto exit
        }

        //TODO FIXME should listen on exist, need wait for timeout
        frameType, data, err := c.readUnpackedResponse()
        if err != nil {
            handleError(w, c, fmt.Sprintf("[%s] error (%s) reading response %d %s", c, err.Error(), frameType, data))
            continue
        }

        switch frameType {
        case FrameTypeMessage:
            msg, err := broker.DecodeMessage(data)
            // msg.cmdChan = c.cmdChan
            // msg.responseChan = c.finishedMessages

            if err != nil {
                handleError(w, c, fmt.Sprintf("[%s] error (%s) reading response %d %s", c, err.Error(), frameType, data))
                continue
            }
            log.Debug("INFO: [%s] FrameTypeMessage receive  %s - %s", c, msg.Id, msg.Body)

            // remain := atomic.AddInt64(&c.rdyCount, -1)
            // atomic.AddInt64(&q.totalRdyCount, -1)
            // atomic.AddUint64(&c.messagesReceived, 1)
            // atomic.AddUint64(&q.MessagesReceived, 1)
            // atomic.AddInt64(&c.messagesInFlight, 1)
            // atomic.AddInt64(&q.messagesInFlight, 1)
            // atomic.StoreInt64(&c.lastMsgTimestamp, time.Now().UnixNano())

            // if q.VerboseLogging {
            // 	log.Debug("[%s] (remain %d) FrameTypeMessage: %s - %s",
            // 		c, remain, msg.Id, msg.Body)
            // }

            // q.incomingMessages <- msg
            // c.rdyChan <- c
        case FrameTypeResponse:
            switch {
            case bytes.Equal(data, []byte("CLOSE_WAIT")):
                // server is ready for us to close (it ack'd our StartClose)
                // we can assume we will not receive any more messages over this channel
                // (but we can still write back responses)
                log.Debug("[%s] received ACK from nsqd - now in CLOSE_WAIT", c)
                atomic.StoreInt32(&c.stopFlag, 1)
            case bytes.Equal(data, []byte("H")):
                // var buf bytes.Buffer
                // err := c.sendCommand(&buf, Nop())
                // if err != nil {
                // 	handleError(q, c, fmt.Sprintf("[%s] error sending NOP - %s",
                // 		c, err.Error()))
                // 	goto exit
                // }
                log.Debug("[%s] heartbeat received", c)
            }
        case FrameTypeAck:
            log.Debug("[%s] ack receive %s", c, data)
            params := bytes.Split(data, separatorBytes)
            ackType, err := strconv.ParseInt(string(params[0]), 10, 64)
            clientId, err := strconv.ParseInt(string(params[1]), 10, 64)
            msgId, err := strconv.ParseInt(string(params[1]), 10, 64)
            if err != nil {
                log.Debug("ERROR: parse msgId error %s", err)
                break
            }

            if ackType != int64(ACK_SUCCESS) {
                model.SaveOfflineMessage(clientId, msgId)
                model.IncrMsgErrCount(msgId, 1)
                model.IncrClientErrCount(clientId, 1)
            } else {
                model.IncrMsgOKCount(msgId, 1)
                model.IncrClientOKCount(clientId, 1)
            }

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
        log.Debug("[%s] beginning stopFinishLoop", c)
        close(c.exitChan)
        c.Close()
        go q.cleanupConnection(c)
    })
}

func (q *Worker) cleanupConnection(c *nsqConn) {

    // this blocks until finishLoop and readLoop have exited
    c.wg.Wait()

    q.Lock()
    delete(q.nsqConnections, c.String())
    left := len(q.nsqConnections)
    q.Unlock()

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
