package main

import (
	"net"

	"code.sohuno.com/kzapp/push-server/util"
)

type admin struct {
	options      *adminOptions
	httpAddr     *net.TCPAddr
	httpListener net.Listener
	waitGroup    util.WaitGroupWrapper
}

func Newadmin(options *adminOptions) *admin {
	httpAddr, err := net.ResolveTCPAddr("tcp", options.HTTPAddress)
	if err != nil {
		log.Fatal(err)
	}

	return &admin{
		options:  options,
		httpAddr: httpAddr,
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
}

func (n *admin) Exit() {
	n.httpListener.Close()
	n.waitGroup.Wait()
}
