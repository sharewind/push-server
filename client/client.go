package client

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"code.sohuno.com/kzapp/push-server/broker"
)

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

// WriterTransaction is returned by the async publish methods
// to retrieve metadata about the command after the
// response is received.
// type WriterTransaction struct {
// 	cmd       *Command
// 	doneChan  chan *WriterTransaction
// 	FrameType int32         // the frame type received in response to the publish command
// 	Data      []byte        // the response data of the publish command
// 	Error     error         // the error (or nil) of the publish command
// 	Args      []interface{} // the slice of variadic arguments passed to PublishAsync or MultiPublishAsync
// }

// func (t *WriterTransaction) finish() {
// 	if t.doneChan != nil {
// 		t.doneChan <- t
// 	}
// }

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
		WriteTimeout:      time.Second,
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

// PublishAsync publishes a message body to the specified topic
// but does not wait for the response from `nsqd`.
//
// When the Writer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `WriterTransaction` instance with the supplied variadic arguments
// (and the response `FrameType`, `Data`, and `Error`)
// func (w *Writer) PublishAsync(topic string, body []byte, doneChan chan *WriterTransaction, args ...interface{}) error {
// 	return w.sendCommandAsync(Publish(topic, body), doneChan, args)
// }

// MultiPublishAsync publishes a slice of message bodies to the specified topic
// but does not wait for the response from `nsqd`.
//
// When the Writer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `WriterTransaction` instance with the supplied variadic arguments
// (and the response `FrameType`, `Data`, and `Error`)
// func (w *Writer) MultiPublishAsync(topic string, body [][]byte, doneChan chan *WriterTransaction, args ...interface{}) error {
// 	cmd, err := MultiPublish(topic, body)
// 	if err != nil {
// 		return err
// 	}
// 	return w.sendCommandAsync(cmd, doneChan, args)
// }

// Publish synchronously publishes a message body to the specified topic, returning
// the response frameType, data, and error
// func (w *Writer) Publish(topic string, body []byte) (int32, []byte, error) {
// 	return w.sendCommand(Publish(topic, body))
// }

// MultiPublish synchronously publishes a slice of message bodies to the specified topic, returning
// the response frameType, data, and error
// func (w *Writer) MultiPublish(topic string, body [][]byte) (int32, []byte, error) {
// 	cmd, err := MultiPublish(topic, body)
// 	if err != nil {
// 		return -1, nil, err
// 	}
// 	return w.sendCommand(cmd)
// }

// func (w *Client) sendCommand(cmd *Command) (int32, []byte, error) {
// 	doneChan := make(chan *WriterTransaction)
// 	err := w.sendCommandAsync(cmd, doneChan, nil)
// 	if err != nil {
// 		close(doneChan)
// 		return -1, nil, err
// 	}
// 	t := <-doneChan
// 	return t.FrameType, t.Data, t.Error
// }

// func (w *Writer) sendCommandAsync(cmd *Command, doneChan chan *WriterTransaction, args []interface{}) error {
// 	// keep track of how many outstanding writers we're dealing with
// 	// in order to later ensure that we clean them all up...
// 	atomic.AddInt32(&w.concurrentWriters, 1)
// 	defer atomic.AddInt32(&w.concurrentWriters, -1)

// 	if atomic.LoadInt32(&w.state) != StateConnected {
// 		err := w.connect()
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	t := &WriterTransaction{
// 		cmd:       cmd,
// 		doneChan:  doneChan,
// 		FrameType: -1,
// 		Args:      args,
// 	}

// 	select {
// 	case w.transactionChan <- t:
// 	case <-w.exitChan:
// 		return ErrStopped
// 	}

// 	return nil
// }

func (c *Client) Connect() error {
	if atomic.LoadInt32(&c.stopFlag) == 1 {
		return ErrStopped
	}

	if !atomic.CompareAndSwapInt32(&c.state, StateInit, StateConnected) {
		return ErrNotConnected
	}

	log.Printf("[%s] connecting...", c)
	conn, err := net.DialTimeout("tcp", c.Addr, time.Second*5)
	if err != nil {
		log.Printf("ERROR: [%s] failed to dial %s - %s", c, c.Addr, err)
		atomic.StoreInt32(&c.state, StateInit)
		return err
	}

	c.closeChan = make(chan int)
	c.Conn = conn

	c.SetWriteDeadline(time.Now().Add(c.WriteTimeout))
	_, err = c.Write(MagicV2)
	if err != nil {
		log.Printf("ERROR: [%s] failed to write magic - %s", c, err)
		c.Close()
		return err
	}

	ci := make(map[string]interface{})
	ci["client_id"] = fmt.Sprintf("%d", c.ID)
	ci["heartbeat_interval"] = int64(c.HeartbeatInterval / time.Millisecond)
	ci["feature_negotiation"] = true
	cmd, err := Identify(ci)
	if err != nil {
		log.Printf("ERROR: [%s] failed to create IDENTIFY command - %s", c, err)
		c.Close()
		return err
	}

	c.SetWriteDeadline(time.Now().Add(c.WriteTimeout))
	err = cmd.Write(c)
	if err != nil {
		log.Printf("ERROR: [%s] failed to write IDENTIFY - %s", c, err)
		c.Close()
		return err
	}

	c.SetReadDeadline(time.Now().Add(c.HeartbeatInterval * 2))
	resp, err := ReadResponse(c)
	if err != nil {
		log.Printf("ERROR: [%s] failed to read IDENTIFY response - %s", c, err)
		c.Close()
		return err
	}

	frameType, data, err := UnpackResponse(resp)
	if err != nil {
		log.Printf("ERROR: [%s] failed to unpack IDENTIFY response - %s", c, resp)
		c.Close()
		return err
	}

	if frameType == FrameTypeError {
		log.Printf("ERROR: [%s] IDENTIFY returned error response - %s", c, data)
		c.Close()
		return errors.New(string(data))
	}

	c.wg.Add(2)
	go c.messagePump()
	go c.readLoop()

	return nil
}

func (c *Client) Subscribe(channel_id int64) error {
	cmd := Subscribe(channel_id)
	c.SetWriteDeadline(time.Now().Add(c.WriteTimeout))
	err := cmd.Write(c)
	if err != nil {
		log.Printf("ERROR: [%s] failed to write Subscribe - %s", c, err)
		c.Close()
		return err
	}
	log.Printf("INFO: [%s] success to write Subscribe ", c)
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

// func (w *Writer) transactionCleanup() {
// 	// clean up transactions we can easily account for
// 	for _, t := range w.transactions {
// 		t.Error = ErrNotConnected
// 		t.finish()
// 	}
// 	w.transactions = w.transactions[:0]

// 	// spin and free up any writes that might have raced
// 	// with the cleanup process (blocked on writing
// 	// to transactionChan)
// 	for {
// 		select {
// 		case t := <-w.transactionChan:
// 			t.Error = ErrNotConnected
// 			t.finish()
// 		default:
// 			// keep spinning until there are 0 concurrent writers
// 			if atomic.LoadInt32(&w.concurrentWriters) == 0 {
// 				return
// 			}
// 			// give the runtime a chance to schedule other racing goroutines
// 			time.Sleep(5 * time.Millisecond)
// 			continue
// 		}
// 	}
// }

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
				log.Printf("ERROR: [%s] failed to write HeartBeat - %s", c, err)
				goto exit
			}
			// shoud receive response
			c.SetReadDeadline(time.Now().Add(c.HeartbeatInterval * 2))
		case <-c.exitChan:
			goto exit
		}
	}

exit:
	log.Printf("client: [%s] exiting messagePump", c)
	heartbeatTicker.Stop()
	if err != nil {
		log.Printf("client: [%s] messagePump error - %s", c, err.Error())
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
			// msg.cmdChan = c.cmdChan
			// msg.responseChan = c.finishedMessages

			log.Printf("msg receive %s", msg)
			if err != nil {
				// handleError(q, c, fmt.Sprintf("[%s] error (%s) decoding message %s",
				// 	c, err.Error(), data))
				continue
			}

			// remain := atomic.AddInt64(&c.rdyCount, -1)
			// atomic.AddInt64(&q.totalRdyCount, -1)
			// atomic.AddUint64(&c.messagesReceived, 1)
			// atomic.AddUint64(&q.MessagesReceived, 1)
			// atomic.AddInt64(&c.messagesInFlight, 1)
			// atomic.AddInt64(&q.messagesInFlight, 1)
			// atomic.StoreInt64(&c.lastMsgTimestamp, time.Now().UnixNano())

			// if q.VerboseLogging {
			// 	log.Printf("[%s] (remain %d) FrameTypeMessage: %s - %s",
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
				log.Printf("[%s] received ACK from nsqd - now in CLOSE_WAIT", c)
				atomic.StoreInt32(&c.stopFlag, 1)
			case bytes.Equal(data, []byte("HT")):
				// var buf bytes.Buffer
				log.Printf("[%s] heartbeat received", c)
				// err := c.sendCommand(&buf, Nop())
				// if err != nil {
				// 	handleError(q, c, fmt.Sprintf("[%s] error sending NOP - %s",
				// 		c, err.Error()))
				// 	goto exit
				// }
			default:
				log.Printf("FrameTypeResponse receive %s", string(data))
			}
		case FrameTypeError:
			log.Printf("[%s] error from nsqd %s", c, data)
		default:
			log.Printf("[%s] unknown message type %d", c, frameType)
		}
	}

exit:
	c.wg.Done()
	log.Printf("[%s] readLoop exiting", c)
}
