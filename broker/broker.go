package broker

import (
	"code.sohuno.com/kzapp/push-server/util"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/op/go-logging"
	"net"
	"sync"
	//"strings"
)

const DefaultClientMapSize = 10000

var log = logging.MustGetLogger("broker")

type Broker struct {
	sync.RWMutex
	options *brokerOptions

	//map is clientId_channeId : Client instance
	clients map[string]*client

	tcpAddr      *net.TCPAddr
	httpAddr     *net.TCPAddr
	tcpListener  net.Listener
	httpListener net.Listener
	tlsConfig    *tls.Config

	// notifyChan chan interface{}
	exitChan  chan int
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
		// notifyChan: make(chan interface{}),
		tlsConfig: tlsConfig,
	}

	return b
}

func (b *Broker) Main() {

	context := &context{b}

	// b.waitGroup.Wrap(func() { b.lookupLoop() })

	tcpListener, err := net.Listen("tcp", b.tcpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", b.tcpAddr, err.Error())
	}
	b.tcpListener = tcpListener
	tcpServer := &tcpServer{context: context}
	b.waitGroup.Wrap(func() { util.TCPServer(b.tcpListener, tcpServer) })

	// todo http
	// httpListener, err := net.Listen("tcp", b.httpAddr.String())
	// if err != nil {
	// 	log.Fatalf("FATAL: listen (%s) failed - %s", b.httpAddr, err.Error())
	// }
	// b.httpListener = httpListener
	// httpServer := &httpServer{context: context}
	// b.waitGroup.Wrap(func() { util.HTTPServer(b.httpListener, httpServer) })

	// if b.options.StatsdAddress != "" {
	// 	b.waitGroup.Wrap(func() { b.statsdLoop() })
	// }
}

func (b *Broker) Exit() {

	if b.tcpListener != nil {
		b.tcpListener.Close()
	}
	if b.httpListener != nil {
		b.httpListener.Close()
	}

	// n.Lock()
	// // alreaddy received,  but push failuer need store on offline msg
	// n.Unlock()

	// we want to do this last as it closes the idPump (if closed first it
	// could potentially starve items in process and deadlock)
	close(b.exitChan)
	b.waitGroup.Wait()
}

// AddClient adds a client to the Channel's client list
func (b *Broker) AddClient(clientID int64, channelID int64, client *client) {
	b.Lock()
	defer b.Unlock()

	key := fmt.Sprintf("%d_%d", clientID, channelID)
	log.Debug("put client[%s] = %s", key, client)
	_, ok := b.clients[key]
	if ok {
		return
	}
	b.clients[key] = client
}

// RemoveClient removes a client from the Channel's client list
func (b *Broker) RemoveClient(clientID int64, channelID int64) {
	b.Lock()
	defer b.Unlock()

	key := fmt.Sprintf("%d_%d", clientID, channelID)
	_, ok := b.clients[key]
	if !ok {
		return
	}
	delete(b.clients, key)

	// if len(b.clients) == 0 && b.ephemeralChannel == true {
	// 	go b.deleter.Do(func() { b.deleteCallback(c) })
	// }
}

func (b *Broker) GetClient(clientID int64, channelID int64) (client *client, err error) {
	b.RLock()
	defer b.RUnlock()
	key := fmt.Sprintf("%d_%d", clientID, channelID)
	log.Debug("get client[%s] ", key)
	// log.Debug("%s", b.clients)

	client, ok := b.clients[key]
	if !ok {
		return nil, errors.New("client does not exist")
	}
	return client, nil
}
