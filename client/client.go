package client

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/op/go-logging"
	"net"
	"os"
	"strings"
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
	ID int64
	net.Conn

	WriteTimeout      time.Duration
	Addr              string
	HeartbeatInterval time.Duration
	ShortIdentifier   string
	LongIdentifier    string

	// concurrentWriters int32

	// transactionChan chan *WriterTransaction
	dataChan chan []byte
	// transactions    []*WriterTransaction
	state     int32
	stopFlag  int32
	exitChan  chan int
	closeChan chan int
	wg        sync.WaitGroup
}

// returned when a publish command is made against a Writer that is not connected
var ErrNotConnected = errors.New("not connected")

// returned when a publish command is made against a Writer that has been stopped
var ErrStopped = errors.New("stopped")

// NewWriter returns an instance of Writer for the specified address
func NewClient(addr string, id int64) *Client {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: unable to get hostname %s", err.Error())
	}
	return &Client{
		ID: id,
		// transactionChan: make(chan *WriterTransaction),
		exitChan:  make(chan int),
		closeChan: make(chan int),
		dataChan:  make(chan []byte),

		// can be overriden before connecting
		Addr:              addr,
		WriteTimeout:      5 * time.Second,
		HeartbeatInterval: DefaultClientTimeout / 2,
		ShortIdentifier:   strings.Split(hostname, ".")[0],
		LongIdentifier:    hostname,
	}
}

// String returns the address of the Writer
func (c *Client) String() string {
	return c.Addr
}

// Stop disconnects and permanently stops the Writer
func (c *Client) Stop() {
	if !atomic.CompareAndSwapInt32(&c.stopFlag, 0, 1) {
		return
	}
	c.Close()
	c.wg.Wait()
}

func (c *Client) Connect() error {
	if atomic.LoadInt32(&c.stopFlag) == 1 {
		return ErrStopped
	}

	if !atomic.CompareAndSwapInt32(&c.state, StateInit, StateConnected) {
		return ErrNotConnected
	}

	log.Debug("[%s] connecting...", c)
	conn, err := net.DialTimeout("tcp", c.Addr, time.Second*5)
	if err != nil {
		log.Error("[%s] failed to dial %s - %s", c, c.Addr, err)
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
	ci["client_id"] = fmt.Sprintf("%d", c.ID)
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

	c.wg.Add(2)
	go c.messagePump()
	go c.readLoop()

	return nil
}

func (c *Client) Register(addr string) error {
	endpoint := fmt.Sprintf("http://%s/registration?serial_no=%d&device_type=3&device_name=搜狐Android测试机%d", addr, time.Now().UnixNano(), time.Now().Unix())
	log.Debug("LOOKUPD: querying %s", endpoint)

	data, err := ApiPostRequest(endpoint)
	if err != nil {
		log.Error("Register %s - %s - %s", addr, err.Error(), data)
		return err
	}

	device_id := int64(data.Get("device_id").MustInt())
	c.ID = device_id
	return nil
}

func (c *Client) Subscribe(channel_id int64) error {
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
	close(c.closeChan)
	c.Conn.Close()
	go func() {
		// we need to handle this in a goroutine so we don't
		// block the caller from making progress
		c.wg.Wait()
		atomic.StoreInt32(&c.state, StateInit)
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
				goto exit
			}
			// shoud receive response
			c.SetReadDeadline(time.Now().Add(c.HeartbeatInterval * 2))
		case <-c.exitChan:
			goto exit
		}
	}

exit:
	log.Debug("client: [%s] exiting messagePump", c)
	heartbeatTicker.Stop()
	if err != nil {
		log.Debug("client: [%s] messagePump error - %s", c, err.Error())
	}
}

func (c *Client) readLoop() {
	rbuf := bufio.NewReader(c.Conn)
	for {
		if atomic.LoadInt32(&c.stopFlag) == 1 {
			goto exit
		}

		resp, err := ReadResponse(rbuf)
		frameType, data, err := UnpackResponse(resp)

		if err != nil {
			//handleError(q, c, fmt.Sprintf("[%s] error (%s) reading response %d %s", c, err.Error(), frameType, data))
			continue
		}

		switch frameType {
		case FrameTypeMessage:
			msg, err := broker.DecodeMessage(data)
			log.Info("[%s] FrameTypeMessage receive  %s - %s", c.Conn.RemoteAddr(), msg.Id, msg.Body)
			if err != nil {
				// handleError(q, c, fmt.Sprintf("[%s] error (%s) decoding message %s",
				// 	c, err.Error(), data))
				continue
			}

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
