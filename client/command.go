package client

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	"code.sohuno.com/kzapp/push-server/util"
)

var byteSpace = []byte(" ")
var byteNewLine = []byte("\n")

// Command represents a command from a client to an NSQ daemon
type Command struct {
	Name   []byte
	Params [][]byte
	Body   []byte
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

	for _, param := range c.Params {
		_, err := w.Write(byteSpace)
		if err != nil {
			return err
		}
		_, err = w.Write(param)
		if err != nil {
			return err
		}
	}

	_, err = w.Write(byteNewLine)
	if err != nil {
		return err
	}

	if c.Body != nil {
		bodySize := int32(len(c.Body))
		err := binary.Write(w, binary.BigEndian, &bodySize)
		if err != nil {
			return err
		}
		_, err = w.Write(c.Body)
		if err != nil {
			return err
		}
	}

	return nil
}

// Identify creates a new Command to provide information about the client.  After connecting,
// it is generally the first message sent.
//
// The supplied map is marshaled into JSON to provide some flexibility
// for this command to evolve over time.
//
// See http://bitly.github.io/nsq/clients/tcp_protocol_spec.html#identify for information
// on the supported options
func Identify(js map[string]interface{}) (*Command, error) {
	body, err := json.Marshal(js)
	if err != nil {
		return nil, err
	}
	return &Command{[]byte("IDENTIFY"), nil, body}, nil
}

// Register creates a new Command to add a topic/channel for the connected nsqd
func Register(topic string, channel string) *Command {
	params := [][]byte{[]byte(topic)}
	if len(channel) > 0 {
		params = append(params, []byte(channel))
	}
	return &Command{[]byte("REGISTER"), params, nil}
}

// Unregister creates a new Command to remove a topic/channel for the connected nsqd
func UnRegister(topic string, channel string) *Command {
	params := [][]byte{[]byte(topic)}
	if len(channel) > 0 {
		params = append(params, []byte(channel))
	}
	return &Command{[]byte("UNREGISTER"), params, nil}
}

// Ping creates a new Command to keep-alive the state of all the
// announced topic/channels for a given client
func Ping() *Command {
	return &Command{[]byte("PING"), nil, nil}
}

// Publish creates a new Command to write a message to a given topic
func Publish(client_id int64, channel_id string, message_id int64, body []byte) *Command {
	var params = [][]byte{[]byte(fmt.Sprintf("%d", client_id)), []byte(fmt.Sprintf("%d", channel_id)), []byte(fmt.Sprintf("%d", message_id))}
	return &Command{[]byte("PUB"), params, body}
}

// MultiPublish creates a new Command to write more than one message to a given topic.
// This is useful for high-throughput situations to avoid roundtrips and saturate the pipe.
func MultiPublish(topic string, bodies [][]byte) (*Command, error) {
	var params = [][]byte{[]byte(topic)}

	num := uint32(len(bodies))
	bodySize := 4
	for _, b := range bodies {
		bodySize += len(b) + 4
	}
	body := make([]byte, 0, bodySize)
	buf := bytes.NewBuffer(body)

	err := binary.Write(buf, binary.BigEndian, &num)
	if err != nil {
		return nil, err
	}
	for _, b := range bodies {
		err = binary.Write(buf, binary.BigEndian, int32(len(b)))
		if err != nil {
			return nil, err
		}
		_, err = buf.Write(b)
		if err != nil {
			return nil, err
		}
	}

	return &Command{[]byte("MPUB"), params, buf.Bytes()}, nil
}

// Subscribe creates a new Command to subscribe to the given topic/channel
func Subscribe(channel_id string) *Command {
	var params = [][]byte{[]byte(fmt.Sprintf("%d", channel_id))}
	return &Command{[]byte("SUB"), params, nil}
}

// Ready creates a new Command to specify
// the number of messages a client is willing to receive
func Ready(count int) *Command {
	var params = [][]byte{[]byte(strconv.Itoa(count))}
	return &Command{[]byte("RDY"), params, nil}
}

// Finish creates a new Command to indiciate that
// a given message (by id) has been processed successfully
func Finish(id util.HexID) *Command {
	var params = [][]byte{id[:]}
	return &Command{[]byte("FIN"), params, nil}
}

// Requeue creates a new Command to indicate that
// a given message (by id) should be requeued after the given timeout (in ms)
// NOTE: a timeout of 0 indicates immediate requeue
func Requeue(id util.HexID, timeoutMs int) *Command {
	var params = [][]byte{id[:], []byte(strconv.Itoa(timeoutMs))}
	return &Command{[]byte("REQ"), params, nil}
}

// Touch creates a new Command to reset the timeout for
// a given message (by id)
func Touch(id util.HexID) *Command {
	var params = [][]byte{id[:]}
	return &Command{[]byte("TOUCH"), params, nil}
}

// StartClose creates a new Command to indicate that the
// client would like to start a close cycle.  nsqd will no longer
// send messages to a client in this state and the client is expected
// finish pending messages and close the connection
func StartClose() *Command {
	return &Command{[]byte("CLS"), nil, nil}
}

// Nop creates a new Command that has no effect server side.
// Commonly used to respond to heartbeats
func Nop() *Command {
	return &Command{[]byte("NOP"), nil, nil}
}

func HeartBeat() *Command {
	return &Command{[]byte("H"), nil, nil}
}
