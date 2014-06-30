package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"

	"code.sohuno.com/kzapp/push-server/util"
)

var byteSpace = []byte(" ")
var byteNewLine = []byte("\n")
var byteParamLenPrefix = []byte("*")
var byteValueLenPrefix = []byte("$")

// Command represents a command from a client to an NSQ daemon
type Command struct {
	Name   []byte
	Params [][]byte
}

// String returns the name and parameters of the Command
func (c *Command) String() string {
	if len(c.Params) > 0 {
		return fmt.Sprintf("%s %s", c.Name, string(bytes.Join(c.Params, byteSpace)))
	}
	return string(c.Name)
}

// Write serializes the Command to the supplied Writer.
//
// It is suggested that the target Writer is buffered to avoid performing many system calls.
func (c *Command) Write(w io.Writer) error {
	_, err := w.Write(c.Name)
	if err != nil {
		return err
	}

	// write \n
	_, err = w.Write(byteNewLine)
	if err != nil {
		return err
	}
	// write *
	_, err = w.Write(byteParamLenPrefix)
	if err != nil {
		return err
	}
	// write params len
	len_bytes, _ := util.IntToBytes(len(c.Params))
	_, err = w.Write(len_bytes)
	if err != nil {
		return err
	}

	// write \n
	_, err = w.Write(byteNewLine)
	if err != nil {
		return err
	}

	for _, param := range c.Params {
		// write $
		_, err = w.Write(byteValueLenPrefix)
		if err != nil {
			return err
		}

		// write params len
		len_bytes, _ := util.IntToBytes(len(param))
		_, err = w.Write(len_bytes)
		if err != nil {
			return err
		}
		// write \n
		_, err = w.Write(byteNewLine)
		if err != nil {
			return err
		}

		_, err = w.Write(param)
		if err != nil {
			return err
		}

		// write \n
		_, err = w.Write(byteNewLine)
		if err != nil {
			return err
		}
	}

	return nil
}

func ParseCommand(reader *bufio.Reader) (*Command, error) {
	log.Printf("parseCommand ....")

	var params [][]byte
	cmdName, _, err := reader.ReadLine()
	log.Printf("%s", cmdName)
	cmdName = []byte(string(cmdName))
	if err != nil {
		return nil, err
	}

	len_bytes, _, err := reader.ReadLine()
	log.Printf("%s", len_bytes)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal([]byte("*"), len_bytes[0:1]) {
		params = [][]byte{len_bytes}
		return &Command{cmdName, params}, nil
	}

	log.Printf("len byte %s", len_bytes[1:])
	param_len, err := util.ByteToInt(len_bytes[1:])
	if err != nil {
		return nil, err
	}

	params = make([][]byte, param_len)
	for i := 0; i < int(param_len); i++ {
		log.Printf("berfore  cmdName=%s, params=%s", cmdName, params)

		len_bytes, _, err := reader.ReadLine()
		log.Printf("param val len = %s", len_bytes)

		log.Printf("cmdName=%s, params=%s", cmdName, params)
		if err != nil {
			return nil, err
		}

		val_len, err := util.ByteToInt(len_bytes[1:])
		// log.Printf("param val len = %d", val_len)
		if err != nil {
			return nil, err
		}

		params[i] = make([]byte, val_len)
		n, err := io.ReadAtLeast(reader, params[i], int(val_len))
		log.Printf("param val = %s", params[i])

		if err != nil || n != int(val_len) {
			return nil, err
		}

		_, _, err = reader.ReadLine()
		if err != nil {
			return nil, err
		}

	}

	log.Printf("cmdName=%s, params=%s", cmdName, params)
	return &Command{cmdName, params}, nil
}

func Conn(client_id int64, heartbeat int64) (*Command, error) {
	return &Command{[]byte("CONN"), [][]byte{[]byte(fmt.Sprintf("%d", client_id)), []byte(fmt.Sprintf("%d", heartbeat))}}, nil
}

// Subscribe creates a new Command to subscribe to the given topic/channel
func Subscribe(topic_id string) *Command {
	var params = [][]byte{[]byte(topic_id)}
	return &Command{[]byte("SUB"), params}
}

// Publish creates a new Command to write a message to a given topic
func Publish(topic_id string, message_id int64, body []byte) *Command {
	var params = [][]byte{[]byte(topic_id), []byte(fmt.Sprintf("%d", message_id)), body}
	return &Command{[]byte("PUB"), params}
}

func Pub(message_id int64, body []byte) *Command {
	var params = [][]byte{[]byte(fmt.Sprintf("%d", message_id)), body}
	return &Command{[]byte("PUB"), params}
}

// client ack message receive
func Ack(client_id int64, topic_id string, message_id int64, body []byte) *Command {
	var params = [][]byte{[]byte(fmt.Sprintf("%d", message_id))}
	return &Command{[]byte("ACK"), params}
}

func HeartBeat() *Command {
	return &Command{[]byte("H"), nil}
}

// Ping creates a new Command to keep-alive the state of all the
// announced topic/channels for a given client
func Ping() *Command {
	return &Command{[]byte("PING"), nil}
}
