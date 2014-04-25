package util

import (
	"github.com/op/go-logging"
	stdlog "log"
	"os"
)

var log = logging.MustGetLogger("util")

func init() {
	// Customize the output format
	logging.SetFormatter(logging.MustStringFormatter("%{level} %{message}"))
	// Setup one stdout and one syslog backend.
	logBackend := logging.NewLogBackend(os.Stderr, "", stdlog.LstdFlags|stdlog.Lshortfile)
	logBackend.Color = true

	// Combine them both into one logging backend.
	logging.SetBackend(logBackend)
}
