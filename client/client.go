package client

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/op/go-logging"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"code.sohuno.com/kzapp/push-server/broker"
	. "code.sohuno.com/kzapp/push-server/util"
)

var log = logging.MustGetLogger("client")

// Writer is a high-level type to publish to NSQ.
//
// A Writer instance is 1:1 with a destination `nsqd`
// and will lazily connect to that instance (and re-connect)
// when Publish commands are executed.
type Client struct {
	ID         int64
	brokerAddr string
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
				log.Info("<%s> regiester to %s success ", c, addr)
				break
			}
			if err != nil {
				log.Debug("<%s> ERROR: failed to connect to %s - %s", c, addr, err.Error())
			}
			log.Debug("[%s] re-connecting in 15 seconds...", addr)
			time.Sleep(15 * time.Second)
		}
		//
		c.ConnectWithRetry()
	}()
}

func (c *Client) Register(addr string, serial_no string) error {
	endpoint := fmt.Sprintf("http://%s/registration?serial_no=%s&device_type=3&device_name=搜狐Android测试机%d", addr, serial_no, time.Now().Unix())
	log.Debug("LOOKUPD: querying %s", endpoint)

	data, err := ApiPostRequest(endpoint)
	if err != nil {
		log.Error("Register %s - %s - %s", addr, err.Error(), data)
		return err
	}

	device_id := int64(data.Get("device_id").MustInt())
	broker_addr := string(data.Get("broker").MustString())
	c.ID = device_id
	c.brokerAddr = broker_addr
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
				log.Info("connect to %s success ", c.brokerAddr)
				break
			}

			if err != nil {
				log.Debug("<%s> ERROR: failed to connect to %s - %s", c, c.brokerAddr, err.Error())
			}

			log.Debug("<%s>  re-connecting [%s] in 15 seconds...", c, c.brokerAddr)
			time.Sleep(15 * time.Second)
		}

		c.Subscribe(c.SubChannel)
	}()
}

func (c *Client) Connect() error {
	if atomic.LoadInt32(&c.stopFlag) == 1 {
		return ErrStopped
	}

	if !atomic.CompareAndSwapInt32(&c.state, StateInit, StateConnected) {
		return ErrNotConnected
	}

	log.Debug("[%s] connecting to %s .....", c, c.brokerAddr)
	conn, err := net.DialTimeout("tcp", c.brokerAddr, time.Second*5)
	if err != nil {
		log.Error("[%s] failed to dial %s - %s", c, c.brokerAddr, err)
		atomic.StoreInt32(&c.state, StateInit)
		return err
	}

	c.closeChan = make(chan int)
	c.Conn = conn

	c.SetWriteDeadline(time.Now().Add(c.WriteTimeout))
	_, err = c.Write(MagicV1)
	if err != nil {
		log.Error("[%s] failed to write magic - %s", c, err)
		c.Close()
		return err
	}

	ci := make(map[string]interface{})
	ci["client_id"] = c.ID
	ci["heartbeat_interval"] = int64(c.HeartbeatInterval / time.Millisecond)
	ci["feature_negotiation"] = true
	ci["role"] = "client"
	cmd, err := Identify(ci)
	if err != nil {
		log.Error("[%s] failed to create IDENTIFY command - %s", c, err)
		c.Close()
		return err
	}

	c.SetWriteDeadline(time.Now().Add(c.WriteTimeout))
	err = cmd.Write(c)
	if err != nil {
		log.Error("[%s] failed to write IDENTIFY - %s", c, err)
		c.Close()
		return err
	}

	c.SetReadDeadline(time.Now().Add(c.HeartbeatInterval * 2))
	resp, err := ReadResponse(c)
	if err != nil {
		log.Error("[%s] failed to read IDENTIFY response - %s", c, err)
		c.Close()
		return err
	}

	frameType, data, err := UnpackResponse(resp)
	if err != nil {
		log.Error("[%s] failed to unpack IDENTIFY response - %s", c, resp)
		c.Close()
		return err
	}

	if frameType == FrameTypeError {
		log.Error("[%s] IDENTIFY returned error response - %s", c, data)
		c.Close()
		return errors.New(string(data))
	}

	c.stopper = sync.Once{}
	c.exitChan = make(chan int)

	c.wg.Add(2)
	go c.messagePump()
	go c.readLoop()

	return nil
}

func (c *Client) Subscribe(channel_id string) error {
	cmd := Subscribe(channel_id)
	c.SetWriteDeadline(time.Now().Add(c.WriteTimeout))
	err := cmd.Write(c)
	if err != nil {
		log.Error("[%s] failed to write Subscribe - %s", c, err)
		c.Close()
		return err
	}
	log.Info("[%s] success to write Subscribe ", c)
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
		// log.Debug("set client to StateInit ")
	}()
}

func (c *Client) messagePump() {
	var err error

	heartbeatTicker := time.NewTicker(c.HeartbeatInterval)
	heartbeatChan := heartbeatTicker.C

	for {
		select {
		case <-heartbeatChan:
			cmd := HeartBeat()
			c.SetWriteDeadline(time.Now().Add(c.WriteTimeout))
			err = cmd.Write(c)
			if err != nil {
				log.Error("[%s] failed to write HeartBeat - %s", c, err)
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
		log.Debug("client: [%s] messagePump error - %s", c, err.Error())
	}
	c.wg.Done()
	log.Debug("client: [%s] exiting messagePump", c)
}

var MessageCount uint64 = 0

func (c *Client) readLoop() {
	rbuf := bufio.NewReader(c.Conn)
	for {
		if atomic.LoadInt32(&c.stopFlag) == 1 {
			log.Info("[%s] stopBrokerConn on client stopFlag ", c.LocalAddr().String())
			c.stopBrokerConn()
			goto exit
		}

		resp, err := ReadResponse(rbuf)
		frameType, data, err := UnpackResponse(resp)

		if err != nil {
			handleError(c, fmt.Sprintf("[%s] error (%s) reading response %d %s", c, err.Error(), frameType, data))
			continue
		}

		switch frameType {
		case FrameTypeMessage:
			msg, err := broker.DecodeMessage(data)
			if err != nil {
				handleError(c, fmt.Sprintf("[%s] error (%s) decoding message %s", c, err.Error(), data))
				continue
			}

			atomic.AddUint64(&MessageCount, 1)
			log.Info("[%s] FrameTypeMessage receive %d  %s - %s", c.Conn.RemoteAddr(), atomic.LoadUint64(&MessageCount), msg.Id, msg.Body)

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
				log.Debug("[%s] heartbeat received", c)
			default:
				log.Debug("FrameTypeResponse receive %s", string(data))
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

func handleError(c *Client, errMsg string) {
	log.Debug("[%s] handleError %s", c, errMsg)
	atomic.StoreInt32(&c.stopFlag, 1)

	go func() {
		log.Debug("[%s] re-connecting in 15 seconds...", c.brokerAddr)
		time.Sleep(15 * time.Second)

		atomic.StoreInt32(&c.stopFlag, 0)
		c.ConnectWithRetry()
	}()
	// }
}

func (c *Client) stopBrokerConn() {
	c.stopper.Do(func() {
		log.Debug("c.stopper done................")
		close(c.exitChan)
		c.Close()
		log.Debug("[%s] stopBrokerConn!", c)
	})
}
