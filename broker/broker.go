package broker

import (
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

const DefaultClientMapSize = 1000000

var log = logging.MustGetLogger("broker")

type PubMessage struct {
	clientID  int64
	messageID int64
	channelID string
	body      []byte
	pubClient *client //TODO should be a finishedChan
}

type Broker struct {
	sync.RWMutex
	options *brokerOptions

	MessageCount  uint64
	FinishedCount uint64
	ErrorCount    uint64
	ClientCount   uint64

	//map is clientId_channeId : Client instance
	clients map[string]*client

	tcpAddr      *net.TCPAddr
	httpAddr     *net.TCPAddr
	tcpListener  net.Listener
	httpListener net.Listener
	tlsConfig    *tls.Config

	exitChan  chan int
	idChan    chan int64
	pubChan   chan *PubMessage
	waitGroup util.WaitGroupWrapper
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
		clients:  make(map[string]*client, DefaultClientMapSize), //default client map size
		exitChan: make(chan int),
		idChan:   make(chan int64, 4096),
		pubChan:  make(chan *PubMessage, 1000000),
		// notifyChan: make(chan interface{}),
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
	b.waitGroup.Wrap(func() { util.TCPServer(b.tcpListener, tcpServer) })

	httpListener, err := net.Listen("tcp", b.httpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", b.httpAddr, err.Error())
	}

	b.httpListener = httpListener
	httpServer := &httpServer{context: context}
	b.waitGroup.Wrap(func() { util.HTTPServer(b.httpListener, httpServer) })
	b.waitGroup.Wrap(func() { b.idPump() })

	for i := 0; i < runtime.NumCPU(); i++ {
		b.waitGroup.Wrap(func() { b.router() })
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
func (b *Broker) AddClient(clientID int64, channelID string, client *client) {
	b.Lock()
	defer b.Unlock()

	key := fmt.Sprintf("%d_%d", clientID, channelID)
	// log.Debug("put client[%s] = %s", key, client)
	_, ok := b.clients[key]
	if ok {
		return
	}
	b.clients[key] = client
}

// RemoveClient removes a client from the Channel's client list
func (b *Broker) RemoveClient(clientID int64, channelID string) {
	b.Lock()
	defer b.Unlock()

	key := fmt.Sprintf("%d_%d", clientID, channelID)
	_, ok := b.clients[key]
	if !ok {
		return
	}
	delete(b.clients, key)
}

func (b *Broker) GetClient(clientID int64, channelID string) (client *client, err error) {
	b.RLock()
	defer b.RUnlock()
	key := fmt.Sprintf("%d_%d", clientID, channelID)
	// log.Debug("get client[%s] ", key)

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
