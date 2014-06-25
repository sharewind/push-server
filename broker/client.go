package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const DefaultBufferSize = 512

type identifyDataV2 struct {
	ClientID            int64 `json:"client_id"`
	HeartbeatInterval   int   `json:"heartbeat_interval"`
	OutputBufferSize    int   `json:"output_buffer_size"`
	OutputBufferTimeout int   `json:"output_buffer_timeout"`
	MsgTimeout          int   `json:"msg_timeout"`
}

type ClientResponse struct {
	response  []byte
	err       error
	frameType int32
}

type client struct {
	sync.RWMutex

	context   *context
	UserAgent string
	Role      string

	// original connection
	net.Conn

	// reading/writing interfaces
	Reader *bufio.Reader
	Writer *bufio.Writer

	OutputBufferSize    int
	OutputBufferTimeout time.Duration

	HeartbeatInterval time.Duration

	MsgTimeout time.Duration

	State       int32
	ConnectTime time.Time

	ExitChan chan int
	stopFlag int32
	stopper  sync.Once

	ClientID int64
	Hostname string

	SubChannel    string
	clientMsgChan chan *Message
	responseChan  chan *ClientResponse

	// re-usable buffer for reading the 4-byte lengths off the wire
	lenBuf   [4]byte
	lenSlice []byte
}

func newClient(conn net.Conn, context *context) *client {
	var identifier string
	if conn != nil {
		identifier, _, _ = net.SplitHostPort(conn.RemoteAddr().String())
	}

	c := &client{
		context:       context,
		clientMsgChan: make(chan *Message, 1),
		responseChan:  make(chan *ClientResponse, 1),
		Conn:          conn,

		Reader: bufio.NewReaderSize(conn, DefaultBufferSize),
		Writer: bufio.NewWriterSize(conn, DefaultBufferSize),

		OutputBufferSize:    DefaultBufferSize,
		OutputBufferTimeout: 250 * time.Millisecond,

		// ReadyStateChan has a buffer of 1 to guarantee that in the event
		// there is a race the state update is not lost
		ExitChan:    make(chan int),
		ConnectTime: time.Now(),
		// State:          broker.StateInit,

		ClientID: -1,
		Hostname: identifier,

		// heartbeats are client configurable but default to 30s
		HeartbeatInterval: context.broker.options.ClientTimeout / 2,
	}
	c.lenSlice = c.lenBuf[:]
	return c
}

func (c *client) String() string {
	return c.RemoteAddr().String()
}

func (c *client) Identify(data identifyDataV2) error {
	c.Lock()
	c.ClientID = data.ClientID
	c.Unlock()

	err := c.SetHeartbeatInterval(data.HeartbeatInterval)
	if err != nil {
		return err
	}

	return nil
}

func (c *client) StartClose() {
	// mark this client as closing
	atomic.StoreInt32(&c.State, StateClosing)
}

func (c *client) SetHeartbeatInterval(desiredInterval int) error {
	c.Lock()
	defer c.Unlock()

	switch {
	case desiredInterval == -1:
		c.HeartbeatInterval = 0
	case desiredInterval == 0:
		// do nothing (use default)
	case desiredInterval >= 1000:
		c.HeartbeatInterval = time.Duration(desiredInterval) * time.Millisecond
	default:
		return errors.New(fmt.Sprintf("heartbeat interval (%d) is invalid", desiredInterval))
	}

	return nil
}

func (c *client) Flush() error {
	c.SetWriteDeadline(time.Now().Add(time.Second))

	err := c.Writer.Flush()
	if err != nil {
		return err
	}

	return nil
}
