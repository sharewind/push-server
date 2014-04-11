package main

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"time"

	"code.sohuno.com/kzapp/push-server/util"
)

type admin struct {
	options       *adminOptions
	httpAddr      *net.TCPAddr
	httpListener  net.Listener
	waitGroup     util.WaitGroupWrapper
	notifications chan *AdminAction
}

func Newadmin(options *adminOptions) *admin {
	if len(options.NSQDHTTPAddresses) == 0 && len(options.NSQLookupdHTTPAddresses) == 0 {
		log.Fatalf("--nsqd-http-address or --lookupd-http-address required.")
	}

	if len(options.NSQDHTTPAddresses) != 0 && len(options.NSQLookupdHTTPAddresses) != 0 {
		log.Fatalf("use --nsqd-http-address or --lookupd-http-address not both")
	}

	httpAddr, err := net.ResolveTCPAddr("tcp", options.HTTPAddress)
	if err != nil {
		log.Fatal(err)
	}

	return &admin{
		options:       options,
		httpAddr:      httpAddr,
		notifications: make(chan *AdminAction),
	}
}

func (n *admin) handleAdminActions() {
	for action := range n.notifications {
		content, err := json.Marshal(action)
		if err != nil {
			log.Debug("Error serializing admin action! %s", err)
		}
		httpclient := &http.Client{Transport: util.NewDeadlineTransport(10 * time.Second)}
		log.Debug("Posting notification to %s", *notificationHTTPEndpoint)
		_, err = httpclient.Post(*notificationHTTPEndpoint, "application/json", bytes.NewBuffer(content))
		if err != nil {
			log.Debug("Error posting notification: %s", err)
		}
	}
}

func (n *admin) Main() {
	httpListener, err := net.Listen("tcp", n.httpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", n.httpAddr, err.Error())
	}
	n.httpListener = httpListener
	httpServer := NewHTTPServer(&Context{n})
	n.waitGroup.Wrap(func() { util.HTTPServer(n.httpListener, httpServer) })
	n.waitGroup.Wrap(func() { n.handleAdminActions() })
}

func (n *admin) Exit() {
	n.httpListener.Close()
	close(n.notifications)
	n.waitGroup.Wait()
}
