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

	// SetLevel("api")
}

func SetLevel(module string, logLevel *string) {
	switch *logLevel {
	case "debug":
		logging.SetLevel(logging.DEBUG, module)
	case "notice":
		logging.SetLevel(logging.NOTICE, module)
	case "info":
		logging.SetLevel(logging.INFO, module)
	case "error":
		logging.SetLevel(logging.ERROR, module)
	case "warning":
		logging.SetLevel(logging.WARNING, module)
	case "critical":
		logging.SetLevel(logging.CRITICAL, module)
	default:
		logging.SetLevel(logging.NOTICE, module)
	}

}
