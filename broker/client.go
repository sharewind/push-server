package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"code.sohuno.com/kzapp/push-server/model"
)

const DefaultBufferSize = 512

type client struct {
	ClientID int64
	SubTopic string

	sync.RWMutex
	context *context

	// original connection
	net.Conn

	// reading/writing interfaces
	Reader *bufio.Reader
	Writer *bufio.Writer

	// OutputBufferSize    int
	// OutputBufferTimeout time.Duration

	HeartbeatInterval time.Duration
	// MsgTimeout time.Duration

	State       int32
	ConnectTime time.Time

	ExitChan chan int
	stopFlag int32
	stopper  sync.Once

	clientMsgChan chan *model.Message
	responseChan  chan *Response

	// re-usable buffer for reading the 4-byte lengths off the wire
	// lenBuf   [4]byte
	// lenSlice []byte
}

func newClient(conn net.Conn, context *context) *client {
	c := &client{
		context:       context,
		clientMsgChan: make(chan *model.Message, 1),
		responseChan:  make(chan *Response, 1),
		Conn:          conn,

		Reader: bufio.NewReaderSize(conn, DefaultBufferSize),
		Writer: bufio.NewWriterSize(conn, DefaultBufferSize),

		// OutputBufferSize:    DefaultBufferSize,
		// OutputBufferTimeout: 250 * time.Millisecond,

		// ReadyStateChan has a buffer of 1 to guarantee that in the event
		// there is a race the state update is not lost
		ExitChan:    make(chan int),
		ConnectTime: time.Now(),
		State:       StateInit,

		ClientID: -1,
		// heartbeats are client configurable but default to 30s
		HeartbeatInterval: context.broker.options.ClientTimeout / 2,
	}
	// c.lenSlice = c.lenBuf[:]
	return c
}

func (c *client) String() string {
	return fmt.Sprintf("client_%d@[%s]", c.ClientID, c.RemoteAddr().String())
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
