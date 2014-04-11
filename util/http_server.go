package util

import (
	"net"
	"net/http"
	"strings"
)

func HTTPServer(listener net.Listener, handler http.Handler) {
	log.Debug("HTTP: listening on %s", listener.Addr().String())

	server := &http.Server{
		Handler: handler,
	}
	err := server.Serve(listener)
	// theres no direct way to detect this error because it is not exposed
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		log.Debug("ERROR: http.Serve() - %s", err.Error())
	}

	log.Debug("HTTP: closing %s", listener.Addr().String())
}
