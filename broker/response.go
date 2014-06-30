package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"

	"code.sohuno.com/kzapp/push-server/util"
)

var byteRespOK = []byte("+")
var byteRespErr = []byte("-")

var RespOK = &Response{true, []byte("OK")}
var RespInvalidClient = &Response{false, []byte("invalid_client")}
var RespInvalidTopic = &Response{false, []byte("invalid_topic")}
var RespSubAck = &Response{true, []byte("SUBACK")}
var RespPubAck = &Response{true, []byte("PUBACK")}
var RespConnAck = &Response{true, []byte("CONNACK")}
var RespHeartbeat = &Response{true, []byte("H")}

// Command represents a command from a client to an NSQ daemon
type Response struct {
	OK   bool
	Body []byte
}

// String returns the name and parameters of the Command
func (r *Response) String() string {
	if r.OK {
		return fmt.Sprintf("+%s", r.Body)
	} else {
		return fmt.Sprintf("-%s", r.Body)
	}
}

// Write serializes the Command to the supplied Writer.
//
// It is suggested that the target Writer is buffered to avoid performing many system calls.
func (r *Response) Write(w io.Writer) error {
	var err error

	if r.OK {
		_, err = w.Write(byteRespOK)
	} else {
		_, err = w.Write(byteRespErr)
	}
	if err != nil {
		return err
	}

	//
	_, err = w.Write(r.Body)
	if err != nil {
		return err
	}

	// write \n
	_, err = w.Write(byteNewLine)
	if err != nil {
		return err
	}
	return nil
}

func ParseResponse(reader *bufio.Reader) (*Response, error) {
	resp_bytes, _, err := reader.ReadLine()
	if err != nil {
		return nil, err
	}

	if bytes.Equal(byteRespOK, resp_bytes[0:1]) {
		return &Response{true, resp_bytes[1:]}, nil
	} else {
		return &Response{false, resp_bytes[1:]}, nil
	}
}

func ParseRespCommand(reader *bufio.Reader) (interface{}, error) {
	var params [][]byte
	cmdName, _, err := reader.ReadLine()
	if err != nil {
		return nil, err
	}

	if bytes.Equal(byteRespOK, cmdName[0:1]) {
		return &Response{true, cmdName[1:]}, nil

	} else if bytes.Equal(byteRespErr, cmdName[0:1]) {
		return &Response{false, cmdName[1:]}, nil
	}

	len_bytes, _, err := reader.ReadLine()
	if err != nil {
		return nil, err
	}

	if !bytes.Equal([]byte("*"), len_bytes[0:1]) {
		params = [][]byte{len_bytes}
		return &Command{cmdName, params}, nil
	}

	param_len, err := util.ByteToInt(len_bytes[1:])
	if err != nil {
		return nil, err
	}

	params = make([][]byte, param_len)
	for i := 0; i < int(param_len); i++ {
		len_bytes, _, err := reader.ReadLine()
		if err != nil {
			return nil, err
		}

		val_len, err := util.ByteToInt(len_bytes[1:])
		if err != nil {
			return nil, err
		}

		params[i] = make([]byte, val_len)
		n, err := io.ReadAtLeast(reader, params[i], int(val_len))
		if err != nil || n != int(val_len) {
			return nil, err
		}

		_, _, err = reader.ReadLine()
		if err != nil {
			return nil, err
		}

	}
	return &Command{cmdName, params}, nil
}
