package main

import (
	"github.com/op/go-logging"
	stdlog "github.com/op/go-logging"
	"os"
)

var log = logging.MustGetLogger("github.com/op/go-logging")

func init() {
	// Customize the output format
	logging.SetFormatter(logging.MustStringFormatter("▶ %{level} 0x%{id:x} %{message}"))
	// Setup one stdout and one syslog backend.
	logBackend := logging.NewLogBackend(os.Stderr, "", stdlog.LstdFlags|stdlog.Lshortfile)
	logBackend.Color = true
	syslogBackend, err := logging.NewSyslogBackend("")
	if err != nil {
		log.Fatal(err)
	}

	// Combine them both into one logging backend.
	logging.SetBackend(logBackend, syslogBackend)
}
