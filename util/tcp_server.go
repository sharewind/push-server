package util

import (
	"net"
	"runtime"
	"strings"
)

type TCPHandler interface {
	Handle(net.Conn)
}

func TCPServer(listener net.Listener, handler TCPHandler) {
	log.Debug("TCP: listening on %s", listener.Addr().String())

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				log.Debug("NOTICE: temporary Accept() failure - %s", err.Error())
				runtime.Gosched()
				continue
			}
			// theres no direct way to detect this error because it is not exposed
			if !strings.Contains(err.Error(), "use of closed network connection") {
				log.Debug("ERROR: listener.Accept() - %s", err.Error())
			}
			break
		}
		go handler.Handle(clientConn)
	}

	log.Debug("TCP: closing %s", listener.Addr().String())
}
