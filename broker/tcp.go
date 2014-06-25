package main

import (
	"io"
	"log"
	"net"

	"code.sohuno.com/kzapp/push-server/util"
)

type tcpServer struct {
	context *context
}

func (p *tcpServer) Handle(clientConn net.Conn) {
	// log.Debug("TCP: new client(%s)", clientConn.RemoteAddr())

	// The client should initialize itself by sending a 4 byte sequence indicating
	// the version of the protocol that it intends to communicate, this will allow us
	// to gracefully upgrade the protocol away from text/line oriented to whatever...
	buf := make([]byte, 4)
	_, err := io.ReadFull(clientConn, buf)
	if err != nil {
		if clientConn != nil {
			clientConn.Close()
		}
		log.Printf("failed to read protocol version - %s", err.Error())
		return
	}
	protocolMagic := string(buf)

	// log.Debug("CLIENT(%s): desired protocol magic '%s'", clientConn.RemoteAddr(), protocolMagic)

	var prot util.Protocol
	switch protocolMagic {
	case "  V1":
		prot = &protocol{context: p.context}
	default:
		util.SendFramedResponse(clientConn, util.FrameTypeError, []byte("E_BAD_PROTOCOL"))
		clientConn.Close()
		log.Printf("client(%s) bad protocol magic '%s'", clientConn.RemoteAddr(), protocolMagic)
		return
	}

	err = prot.IOLoop(clientConn)
	if err != nil {
		log.Printf("client(%s) - %s", clientConn.RemoteAddr(), err.Error())
		return
	}
}
