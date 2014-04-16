package broker

import (
	//"code.sohuno.com/kzapp/push-server/util"
	"bufio"
	"compress/flate"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/mreiferson/go-snappystream"
	// "github.com/op/go-logging"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const DefaultBufferSize = 16 * 1024

type identifyDataV2 struct {
	ClientID            string `json:"client_id"`
	Role                string `json:"role"`
	Hostname            string `json:"hostname"`
	HeartbeatInterval   int    `json:"heartbeat_interval"`
	OutputBufferSize    int    `json:"output_buffer_size"`
	OutputBufferTimeout int    `json:"output_buffer_timeout"`
	FeatureNegotiation  bool   `json:"feature_negotiation"`
	TLSv1               bool   `json:"tls_v1"`
	Deflate             bool   `json:"deflate"`
	DeflateLevel        int    `json:"deflate_level"`
	Snappy              bool   `json:"snappy"`
	SampleRate          int32  `json:"sample_rate"`
	UserAgent           string `json:"user_agent"`
	MsgTimeout          int    `json:"msg_timeout"`
}

type client struct {
	// ReadyCount     int64
	// LastReadyCount int64
	// InFlightCount  int64
	// MessageCount   uint64
	// FinishCount    uint64
	// RequeueCount   uint64

	sync.RWMutex

	context   *context
	UserAgent string
	Role      string

	// original connection
	net.Conn

	// connections based on negotiated features
	tlsConn     *tls.Conn
	flateWriter *flate.Writer

	// reading/writing interfaces
	Reader *bufio.Reader
	Writer *bufio.Writer

	OutputBufferSize    int
	OutputBufferTimeout time.Duration

	HeartbeatInterval time.Duration

	MsgTimeout time.Duration

	State       int32
	ConnectTime time.Time
	// Channel        *Channel
	// incomingMsgChan chan *Message // TODO need process message send error, message unsend on closing
	ReadyStateChan chan int
	ExitChan       chan int

	ClientID   int64
	Hostname   string
	SubChannel int64

	SampleRate int32

	// IdentifyEventChan chan identifyEvent
	// SubEventChan      chan *Channel

	TLS     int32
	Snappy  int32
	Deflate int32

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
		context: context,

		Conn: conn,

		Reader: bufio.NewReaderSize(conn, DefaultBufferSize),
		Writer: bufio.NewWriterSize(conn, DefaultBufferSize),

		OutputBufferSize:    DefaultBufferSize,
		OutputBufferTimeout: 250 * time.Millisecond,

		MsgTimeout: context.broker.options.MsgTimeout,

		// ReadyStateChan has a buffer of 1 to guarantee that in the event
		// there is a race the state update is not lost
		ReadyStateChan: make(chan int, 1),
		ExitChan:       make(chan int),
		ConnectTime:    time.Now(),
		// State:          broker.StateInit,

		ClientID: -1,
		Hostname: identifier,

		// SubEventChan:      make(chan *Channel, 1),
		// IdentifyEventChan: make(chan identifyEvent, 1),

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
	hostname := data.Hostname
	clientId := data.ClientID

	c.Lock()
	c.ClientID, _ = strconv.ParseInt(clientId, 10, 64)
	c.Hostname = hostname
	c.UserAgent = data.UserAgent
	c.Role = data.Role
	c.Unlock()

	err := c.SetHeartbeatInterval(data.HeartbeatInterval)
	if err != nil {
		return err
	}

	err = c.SetOutputBufferSize(data.OutputBufferSize)
	if err != nil {
		return err
	}

	err = c.SetOutputBufferTimeout(data.OutputBufferTimeout)
	if err != nil {
		return err
	}

	err = c.SetSampleRate(data.SampleRate)
	if err != nil {
		return err
	}

	err = c.SetMsgTimeout(data.MsgTimeout)
	if err != nil {
		return err
	}

	// ie := identifyEvent{
	// 	OutputBufferTimeout: c.OutputBufferTimeout,
	// 	HeartbeatInterval:   c.HeartbeatInterval,
	// 	SampleRate:          c.SampleRate,
	// 	MsgTimeout:          c.MsgTimeout,
	// }

	// // update the client's message pump
	// select {
	// case c.IdentifyEventChan <- ie:
	// default:
	// }

	return nil
}

func (c *client) Stats() ClientStats {
	c.RLock()
	// TODO: deprecated, remove in 1.0
	name := fmt.Sprintf("%s", c.ClientID)

	clientId := fmt.Sprintf("%s", c.ClientID)
	hostname := c.Hostname
	userAgent := c.UserAgent
	c.RUnlock()
	return ClientStats{
		// TODO: deprecated, remove in 1.0
		Name: name,

		Version:       "V2",
		RemoteAddress: c.RemoteAddr().String(),
		ClientID:      clientId,
		Hostname:      hostname,
		UserAgent:     userAgent,
		State:         atomic.LoadInt32(&c.State),
		// ReadyCount:    atomic.LoadInt64(&c.ReadyCount),
		// InFlightCount: atomic.LoadInt64(&c.InFlightCount),
		// MessageCount:  atomic.LoadUint64(&c.MessageCount),
		// FinishCount:   atomic.LoadUint64(&c.FinishCount),
		// RequeueCount: atomic.LoadUint64(&c.RequeueCount),
		ConnectTime: c.ConnectTime.Unix(),
		SampleRate:  atomic.LoadInt32(&c.SampleRate),
		TLS:         atomic.LoadInt32(&c.TLS) == 1,
		Deflate:     atomic.LoadInt32(&c.Deflate) == 1,
		Snappy:      atomic.LoadInt32(&c.Snappy) == 1,
	}
}

func (c *client) StartClose() {
	// Force the client into ready 0
	// c.SetReadyCount(0)
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
	case desiredInterval > 1000:
		// && desiredInterval <= int(c.context.broker.options.MaxHeartbeatInterval/time.Millisecond):
		c.HeartbeatInterval = time.Duration(desiredInterval) * time.Millisecond
	default:
		return errors.New(fmt.Sprintf("heartbeat interval (%d) is invalid", desiredInterval))
	}

	return nil
}

func (c *client) SetOutputBufferSize(desiredSize int) error {
	var size int

	switch {
	case desiredSize == -1:
		// effectively no buffer (every write will go directly to the wrapped net.Conn)
		size = 1
	case desiredSize == 0:
		// do nothing (use default)
	case desiredSize >= 64 && desiredSize <= int(c.context.broker.options.MaxOutputBufferSize):
		size = desiredSize
	default:
		return errors.New(fmt.Sprintf("output buffer size (%d) is invalid", desiredSize))
	}

	if size > 0 {
		c.Lock()
		defer c.Unlock()
		c.OutputBufferSize = size
		err := c.Writer.Flush()
		if err != nil {
			return err
		}
		c.Writer = bufio.NewWriterSize(c.Conn, size)
	}

	return nil
}

func (c *client) SetOutputBufferTimeout(desiredTimeout int) error {
	c.Lock()
	defer c.Unlock()

	switch {
	case desiredTimeout == -1:
		c.OutputBufferTimeout = 0
	case desiredTimeout == 0:
		// do nothing (use default)
	case desiredTimeout >= 1 &&
		desiredTimeout <= int(c.context.broker.options.MaxOutputBufferTimeout/time.Millisecond):
		c.OutputBufferTimeout = time.Duration(desiredTimeout) * time.Millisecond
	default:
		return errors.New(fmt.Sprintf("output buffer timeout (%d) is invalid", desiredTimeout))
	}

	return nil
}

func (c *client) SetSampleRate(sampleRate int32) error {
	if sampleRate < 0 || sampleRate > 99 {
		return errors.New(fmt.Sprintf("sample rate (%d) is invalid", sampleRate))
	}
	atomic.StoreInt32(&c.SampleRate, sampleRate)
	return nil
}

func (c *client) SetMsgTimeout(msgTimeout int) error {
	c.Lock()
	defer c.Unlock()

	switch {
	case msgTimeout == 0:
		// do nothing (use default)
	case msgTimeout >= 1000 &&
		msgTimeout <= int(c.context.broker.options.MaxMsgTimeout/time.Millisecond):
		c.MsgTimeout = time.Duration(msgTimeout) * time.Millisecond
	default:
		return errors.New(fmt.Sprintf("msg timeout (%d) is invalid", msgTimeout))
	}

	return nil
}

func (c *client) UpgradeTLS() error {
	c.Lock()
	defer c.Unlock()

	tlsConn := tls.Server(c.Conn, c.context.broker.tlsConfig)
	err := tlsConn.Handshake()
	if err != nil {
		return err
	}
	c.tlsConn = tlsConn

	c.Reader = bufio.NewReaderSize(c.tlsConn, DefaultBufferSize)
	c.Writer = bufio.NewWriterSize(c.tlsConn, c.OutputBufferSize)

	atomic.StoreInt32(&c.TLS, 1)

	return nil
}

func (c *client) UpgradeDeflate(level int) error {
	c.Lock()
	defer c.Unlock()

	conn := c.Conn
	if c.tlsConn != nil {
		conn = c.tlsConn
	}

	c.Reader = bufio.NewReaderSize(flate.NewReader(conn), DefaultBufferSize)

	fw, _ := flate.NewWriter(conn, level)
	c.flateWriter = fw
	c.Writer = bufio.NewWriterSize(fw, c.OutputBufferSize)

	atomic.StoreInt32(&c.Deflate, 1)

	return nil
}

func (c *client) UpgradeSnappy() error {
	c.Lock()
	defer c.Unlock()

	conn := c.Conn
	if c.tlsConn != nil {
		conn = c.tlsConn
	}

	c.Reader = bufio.NewReaderSize(snappystream.NewReader(conn, snappystream.SkipVerifyChecksum), DefaultBufferSize)
	c.Writer = bufio.NewWriterSize(snappystream.NewWriter(conn), c.OutputBufferSize)

	atomic.StoreInt32(&c.Snappy, 1)

	return nil
}

func (c *client) Flush() error {
	c.SetWriteDeadline(time.Now().Add(time.Second))

	err := c.Writer.Flush()
	if err != nil {
		return err
	}

	if c.flateWriter != nil {
		return c.flateWriter.Flush()
	}

	return nil
}
