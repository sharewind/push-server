package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	broker "code.sohuno.com/kzapp/push-server/broker"
	. "code.sohuno.com/kzapp/push-server/util"
)

const DefaultBufferSize = 512

// Writer is a high-level type to publish to NSQ.
//
// A Writer instance is 1:1 with a destination `nsqd`
// and will lazily connect to that instance (and re-connect)
// when Publish commands are executed.
type Client struct {
	ID         int64
	BrokerAddr string
	SubChannel string

	net.Conn

	WriteTimeout      time.Duration
	HeartbeatInterval time.Duration

	// concurrentWriters int32

	// transactionChan chan *WriterTransaction
	dataChan chan []byte
	// transactions    []*WriterTransaction
	state     int32
	stopFlag  int32
	exitChan  chan int
	closeChan chan int
	wg        sync.WaitGroup
	stopper   sync.Once
}

// returned when a publish command is made against a Writer that is not connected
var ErrNotConnected = errors.New("not connected")

// returned when a publish command is made against a Writer that has been stopped
var ErrStopped = errors.New("stopped")

// NewWriter returns an instance of Writer for the specified address
func NewClient() *Client {
	return &Client{
		// transactionChan: make(chan *WriterTransaction),
		exitChan:  make(chan int),
		closeChan: make(chan int),
		dataChan:  make(chan []byte),

		// can be overriden before connecting
		WriteTimeout:      5 * time.Second,
		HeartbeatInterval: DefaultClientTimeout / 2,
	}
}

// String returns the address of the Writer
func (c *Client) String() string {
	if c.Conn != nil {
		return fmt.Sprintf("%d_[%s]", c.ID, c.LocalAddr().String())
	}
	return fmt.Sprintf("%d", c.ID)
}

// Stop disconnects and permanently stops the Writer
func (c *Client) Stop() {
	if !atomic.CompareAndSwapInt32(&c.stopFlag, 0, 1) {
		return
	}
	c.Close()
	c.wg.Wait()
}

func (c *Client) AutoPump(addr string, subChannelID string, serial_no string) {
	c.SubChannel = subChannelID

	go func() {
		for {
			if atomic.LoadInt32(&c.stopFlag) == 1 {
				break
			}

			err := c.Register(addr, serial_no)
			if err == nil {
				log.Printf("<%s> regiester to %s success ", c, addr)
				break
			}
			if err != nil {
				log.Printf("<%s> ERROR: failed to connect to %s - %s", c, addr, err.Error())
			}
			log.Printf("[%s] re-connecting in 15 seconds...", addr)
			time.Sleep(15 * time.Second)
		}
		//
		c.ConnectWithRetry()
	}()
}

func (c *Client) Register(addr string, serial_no string) error {
	endpoint := fmt.Sprintf("http://%s/registration?serial_no=%s&device_type=3&device_name=搜狐Android测试机%d", addr, serial_no, time.Now().Unix())
	log.Printf("LOOKUPD: querying %s", endpoint)

	data, err := ApiPostRequest(endpoint)
	if err != nil {
		log.Printf("Register %s - %s - %s", addr, err.Error(), data)
		return err
	}

	device_id := int64(data.Get("device_id").MustInt())
	broker_addr := string(data.Get("broker").MustString())
	c.ID = device_id
	c.BrokerAddr = broker_addr
	return nil
}

func (c *Client) ConnectWithRetry() {
	go func() {
		for {
			if atomic.LoadInt32(&c.stopFlag) == 1 {
				break
			}

			err := c.Connect()
			if err == nil {
				log.Printf("connect to %s success ", c.BrokerAddr)
				break
			}

			if err != nil {
				log.Printf("<%s> ERROR: failed to connect to %s - %s", c, c.BrokerAddr, err.Error())
			}

			log.Printf("<%s>  re-connecting [%s] in 15 seconds...", c, c.BrokerAddr)
			time.Sleep(15 * time.Second)
		}

		c.Subscribe(c.SubChannel)
	}()
}

func (c *Client) Connect() error {
	var buf bytes.Buffer

	if atomic.LoadInt32(&c.stopFlag) == 1 {
		return ErrStopped
	}

	if !atomic.CompareAndSwapInt32(&c.state, StateInit, StateConnected) {
		return ErrNotConnected
	}

	log.Printf("[%s] connecting to %s .....", c, c.BrokerAddr)
	conn, err := net.DialTimeout("tcp", c.BrokerAddr, time.Second*5)
	if err != nil {
		log.Printf("[%s] failed to dial %s - %s", c, c.BrokerAddr, err)
		atomic.StoreInt32(&c.state, StateInit)
		return err
	}

	c.closeChan = make(chan int)
	c.Conn = conn
	bufReader := bufio.NewReader(conn)

	c.SetWriteDeadline(time.Now().Add(c.WriteTimeout))
	_, err = c.Write(MagicV1)
	if err != nil {
		log.Printf("[%s] failed to write magic - %s", c, err)
		c.Close()
		return err
	}

	cmd, err := broker.Conn(c.ID, int64(c.HeartbeatInterval/time.Millisecond))
	if err != nil {
		log.Printf("[%s] failed to create CONN command - %s", c, err)
		c.Close()
		return err
	}

	c.SetWriteDeadline(time.Now().Add(c.WriteTimeout))
	cmd.Write(&buf)
	// log.Printf("[%s] conn bytes %s", c, buf.Bytes())

	_, err = c.Conn.Write(buf.Bytes())
	if err != nil {
		log.Printf("[%s] failed to write CONN - %s", c, err)
		c.Close()
		return err
	}

	c.SetReadDeadline(time.Now().Add(c.HeartbeatInterval * 2))
	resp, err := broker.ParseResponse(bufReader)
	if err != nil {
		log.Printf("[%s] failed to read CONN response - %s", c, err)
		c.Close()
		return err
	}

	if !resp.OK {
		log.Printf("[%s] CONN returned error response - %s", c, resp.Body)
		c.Close()
		return errors.New(string(resp.Body))
	}

	c.stopper = sync.Once{}
	c.exitChan = make(chan int)

	c.wg.Add(2)
	go c.messagePump()
	go c.readLoop()

	return nil
}

func (c *Client) Subscribe(channel_id string) error {
	cmd := broker.Subscribe(channel_id)
	c.SetWriteDeadline(time.Now().Add(c.WriteTimeout))
	err := cmd.Write(c)
	if err != nil {
		log.Printf("[%s] failed to write Subscribe - %s", c, err)
		c.Close()
		return err
	}
	log.Printf("[%s] success to write Subscribe ", c)
	return nil
}

func (c *Client) Close() {
	if !atomic.CompareAndSwapInt32(&c.state, StateConnected, StateDisconnected) {
		return
	}
	// close(c.closeChan)
	c.Conn.Close()
	go func() {
		// we need to handle this in a goroutine so we don't
		// block the caller from making progress
		c.wg.Wait()
		atomic.StoreInt32(&c.state, StateInit)
		// log.Printf("set client to StateInit ")
	}()
}

func (c *Client) messagePump() {
	var err error

	heartbeatTicker := time.NewTicker(c.HeartbeatInterval)
	heartbeatChan := heartbeatTicker.C

	for {
		select {
		case <-heartbeatChan:
			cmd := broker.HeartBeat()
			c.SetWriteDeadline(time.Now().Add(c.WriteTimeout))
			err = cmd.Write(c)
			if err != nil {
				log.Printf("[%s] failed to write HeartBeat - %s", c, err)
				c.stopBrokerConn()
				goto exit
			}
			// shoud receive response
			c.SetReadDeadline(time.Now().Add(c.HeartbeatInterval * 2))
		case <-c.exitChan:
			goto exit
		}
	}

exit:
	heartbeatTicker.Stop()
	if err != nil {
		log.Printf("client: [%s] messagePump error - %s", c, err.Error())
	}
	c.wg.Done()
	log.Printf("client: [%s] exiting messagePump", c)
}

var MessageCount uint64 = 0

func (c *Client) readLoop() {
	rbuf := bufio.NewReader(c.Conn)
	for {
		if atomic.LoadInt32(&c.stopFlag) == 1 {
			log.Printf("[%s] stopBrokerConn on client stopFlag ", c.LocalAddr().String())
			c.stopBrokerConn()
			goto exit
		}

		response, err := broker.ParseRespCommand(rbuf)
		if err != nil {
			handleError(c, fmt.Sprintf("[%s] error (%s) reading response ", c, err.Error()))
			continue
		}

		switch val := response.(type) {
		case *broker.Command:
			cmd := val
			if bytes.Equal(cmd.Name, []byte("PUB")) {
				atomic.AddUint64(&MessageCount, 1)
				log.Printf("[%s] FrameTypeMessage receive %d  %s", c.Conn.RemoteAddr(), atomic.LoadUint64(&MessageCount), cmd)

			}

		case *broker.Response:
			resp := val
			switch {
			case bytes.Equal(resp.Body, []byte("H")):
				// var buf bytes.Buffer
				log.Printf("[%s] heartbeat received", c)
			default:
				log.Printf("FrameTypeResponse receive %s", resp)
			}
		}
	}

exit:
	c.wg.Done()
	log.Printf("[%s] readLoop exiting", c)
}

func handleError(c *Client, errMsg string) {
	log.Printf("[%s] handleError %s", c, errMsg)
	atomic.StoreInt32(&c.stopFlag, 1)

	go func() {
		log.Printf("[%s] re-connecting in 15 seconds...", c.BrokerAddr)
		time.Sleep(15 * time.Second)

		atomic.StoreInt32(&c.stopFlag, 0)
		c.ConnectWithRetry()
	}()
	// }
}

func (c *Client) stopBrokerConn() {
	c.stopper.Do(func() {
		log.Printf("c.stopper done................")
		close(c.exitChan)
		c.Close()
		log.Printf("[%s] stopBrokerConn!", c)
	})
}
