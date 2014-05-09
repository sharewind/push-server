package broker

import (
	"code.sohuno.com/kzapp/push-server/model"
	"code.sohuno.com/kzapp/push-server/util"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/op/go-logging"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	//"strings"
)

const DefaultClientMapSize = 200 * 10000

var offlineError = errors.New("client offline error")

var log = logging.MustGetLogger("broker")

type PubMessage struct {
	pubID    int64
	deviceID int64
	msg      *model.Message
}

type Broker struct {
	sync.RWMutex
	options *brokerOptions

	MessageCount  uint64
	FinishedCount uint64
	ErrorCount    uint64
	ClientCount   uint64

	//map is clientId_channeId : Client instance
	clients map[int64]*client

	tcpAddr      *net.TCPAddr
	httpAddr     *net.TCPAddr
	tcpListener  net.Listener
	httpListener net.Listener
	tlsConfig    *tls.Config

	exitChan  chan int
	idChan    chan int64
	waitGroup util.WaitGroupWrapper

	incomingMsgChan   chan *model.Message
	clientPubChan     chan *PubMessage
	clientOfflineChan chan *PubMessage
	// ackChan           chan *model.AckMessage
}

func NewBroker(options *brokerOptions) *Broker {

	var tlsConfig *tls.Config
	if options.MaxDeflateLevel < 1 || options.MaxDeflateLevel > 9 {
		log.Fatalf("--max-deflate-level must be [1,9]")
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", options.TCPAddress)
	if err != nil {
		log.Fatal(err)
	}

	httpAddr, err := net.ResolveTCPAddr("tcp", options.HTTPAddress)
	if err != nil {
		log.Fatal(err)
	}

	if options.TLSCert != "" || options.TLSKey != "" {
		cert, err := tls.LoadX509KeyPair(options.TLSCert, options.TLSKey)
		if err != nil {
			log.Fatalf("ERROR: failed to LoadX509KeyPair %s", err.Error())
		}
		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			ClientAuth:   tls.VerifyClientCertIfGiven,
		}
		tlsConfig.BuildNameToCertificate()
	}

	b := &Broker{
		options:  options,
		tcpAddr:  tcpAddr,
		httpAddr: httpAddr,
		clients:  make(map[int64]*client, DefaultClientMapSize), //default client map size

		exitChan: make(chan int),
		idChan:   make(chan int64, 4096),
		// notifyChan: make(chan interface{}),
		incomingMsgChan:   make(chan *model.Message, 10000),
		clientPubChan:     make(chan *PubMessage, 1024000),
		clientOfflineChan: make(chan *PubMessage, 10240000),
		// backPubChan:       make(chan *client.Command, 1024000),
		// ackChan: make(chan *AckMessage, 1024000),
		tlsConfig: tlsConfig,
	}

	return b
}

func (b *Broker) Main() {

	context := &context{b}

	tcpListener, err := net.Listen("tcp", b.tcpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", b.tcpAddr, err.Error())
	}

	b.tcpListener = tcpListener
	tcpServer := &tcpServer{context: context}

	for i := 0; i < 4; i++ {
		b.waitGroup.Wrap(func() { util.TCPServer(b.tcpListener, tcpServer) })
	}

	httpListener, err := net.Listen("tcp", b.httpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", b.httpAddr, err.Error())
	}

	b.httpListener = httpListener
	httpServer := &httpServer{context: context}
	b.waitGroup.Wrap(func() { util.HTTPServer(b.httpListener, httpServer) })
	b.waitGroup.Wrap(func() { b.idPump() })

	for i := 0; i < 160; i++ {
		b.waitGroup.Wrap(func() { b.router() })
	}
	for i := 0; i < runtime.NumCPU(); i++ {
		// save offline msg
		b.waitGroup.Wrap(func() { b.persisOffline() })
	}
}

func (b *Broker) Exit() {

	if b.tcpListener != nil {
		b.tcpListener.Close()
	}
	if b.httpListener != nil {
		b.httpListener.Close()
	}

	// we want to do this last as it closes the idPump (if closed first it
	// could potentially starve items in process and deadlock)
	close(b.exitChan)
	b.waitGroup.Wait()
}

func (b *Broker) idPump() {
	factory := &util.GuidFactory{}
	lastError := time.Now()
	for {
		id, err := factory.NewGUID(b.options.ID)
		if err != nil {
			now := time.Now()
			if now.Sub(lastError) > time.Second {
				// only print the error once/second
				log.Error("%s", err.Error())
				lastError = now
			}
			runtime.Gosched()
			continue
		}
		select {
		case b.idChan <- id:
		case <-b.exitChan:
			goto exit
		}
	}

exit:
	log.Debug("ID Pump: closing")
}

// AddClient adds a client to the Channel's client list
func (b *Broker) AddClient(clientID int64, client *client) {
	// log.Notice("add client lock")
	b.Lock()
	defer b.Unlock()

	key := clientID //fmt.Sprintf("%d_%s", clientID, channelID)
	_, ok := b.clients[key]
	if ok {
		log.Notice("Warn!! client exist and return [%s] = %s", key, client)
	}
	b.clients[key] = client
	// log.Notice("put client[%d] = %s", key, client)
}

// RemoveClient removes a client from the Channel's client list
func (b *Broker) RemoveClient(clientID int64) {
	// log.Notice("rm client lock")
	b.Lock()
	defer b.Unlock()

	key := clientID //fmt.Sprintf("%d_%s", clientID, channelID)
	_, ok := b.clients[key]
	if !ok {
		log.Notice("client not exist and return [%s]", key)
		return
	}
	delete(b.clients, key)
	// log.Notice("remove client[%d] = %s", key)
}

func (b *Broker) GetClient(clientID int64) (client *client, err error) {
	b.RLock()
	defer b.RUnlock()

	key := clientID //fmt.Sprintf("%d", clientID)
	// log.Notice("get client[%s] ", key)

	client, ok := b.clients[key]
	if !ok {
		return nil, errors.New("client does not exist")
	}
	return client, nil
}

func (b *Broker) GetStats() string {
	b.RLock()
	defer b.RUnlock()

	b.ClientCount = uint64(len(b.clients))
	result := fmt.Sprintf("ClientCount:%d, MessageCount:%d, FinishedCount:%d, ErrorCount:%d ", b.ClientCount, atomic.LoadUint64(&b.MessageCount), atomic.LoadUint64(&b.FinishedCount), atomic.LoadUint64(&b.ErrorCount))
	return result
}

func (b *Broker) ResetStats() {
	b.RLock()
	defer b.RUnlock()

	atomic.StoreUint64(&b.MessageCount, 0)
	atomic.StoreUint64(&b.FinishedCount, 0)
	atomic.StoreUint64(&b.ErrorCount, 0)
}

func (b *Broker) router() {
	log.Debug("router start ..............")
	for {
		select {
		case message := <-b.incomingMsgChan:
			log.Debug("get imcoming %s", message)
			b.produceMessages(message)
		case pub := <-b.clientPubChan:
			// log.Debug("get pub  %s", pub)
			//TODO save pub msg to mongo on stop
			err := b.pushMessage2Client(pub)
			if err != nil {
				b.clientOfflineChan <- pub
			}
			// FIXME should process err on send
		// case ack := <-b.ackChan:
		// 	log.Debug("get ack %s", ack)
		// 	w.processAck(ack)
		case <-b.exitChan:
			goto exit
		}
	}
exit:
	log.Debug("broker exit router")
}

func (b *Broker) persisOffline() {
	log.Debug("persisOffline start ..............")
	for {
		select {
		case pub := <-b.clientOfflineChan:
			// log.Debug("get offline %s", pub)
			//TODO save offline msg to mongo on stop
			err := model.SaveOfflineMessage(pub.deviceID, pub.msg.ID)
			if err != nil {
				log.Error("saveOfflineMessage error %s", err)
			}
		case <-b.exitChan:
			goto exit
		}
	}
exit:
	log.Debug("broker exit persisOffline")
}

// PutMessage writes to the appropriate incoming message channel
func (w *Broker) PutMessage(msg *model.Message) error {
	w.RLock()
	defer w.RUnlock()
	// if atomic.LoadInt32(&t.exitFlag) == 1 {
	// 	return errors.New("exiting")
	// }
	w.incomingMsgChan <- msg

	// atomic.AddUint64(&w.PubCount, 1)
	log.Debug("[worker]<PutMessage> %#v", msg)
	return nil
}

func (w *Broker) produceMessages(message *model.Message) {
	// log.Debug("[worker]<produceMessages> %#v", message)
	// save message on mongodb

	// total, err := model.CountSubscribeByChannelId(message.ChannelID, message.DeviceType)
	// if err != nil {
	// 	log.Error(err.Error())
	// }
	// pageCount := (((total - 1) / 1000) + 1)
	// log.Debug("worker all sum:%d time:%d", total, pageCount)

	lastID := int64(0)
	limit := 1000
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
			// log.Debug("subs %s", sub)
			w.clientPubChan <- &PubMessage{<-w.idChan, sub.DeviceID, message}
			lastID = sub.ID
		}
		log.Debug("get subs page %d  count %d  lastID %d finished!", i, len(subs), lastID)
	}
	log.Debug("get subs_all finished!.........")
}

// func (w *Broker) processAck(ack *model.AckMessage) {
// 	log.Debug("process ack  %s", ack)
// 	if ack.AckType != ACK_SUCCESS {
// 		model.SaveOfflineMessage(ack.DeviceID, ack.MessageID)
// 		model.IncrMsgErrCount(ack.MessageID, 1)
// 		model.IncrClientErrCount(ack.DeviceID, 1)
// 	} else {
// 		model.IncrMsgOKCount(ack.MessageID, 1)
// 		model.IncrClientOKCount(ack.DeviceID, 1)
// 	}
// }

func (w *Broker) pushMessage2Client(pub *PubMessage) (err error) {

	// log.Debug("process on pub %s", pub)
	destClient, err := w.GetClient(pub.deviceID)
	// log.Notice("get client finish %s", pub.deviceID)
	if err != nil || destClient == nil {
		// log.Debug("error %s, client %d is null, params =%s", err, pub.deviceID, pub)
		return offlineError
	}

	// log.Debug("get client %s by channel %s = %s  ", client_id, channel_id, destClient)
	// log.Notice("new message  %s", pub.DeviceID)
	msg := &Message{
		Id:        util.Guid(pub.msg.ID).Hex(),
		Body:      []byte(pub.msg.Body),
		Timestamp: time.Now().UnixNano(),
	}
	destClient.clientMsgChan <- msg
	return nil
}
