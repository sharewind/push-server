package main

import (
	"github.com/op/go-logging"
	"net/http"
	"net/rpc"
	"os"
	"runtime"
	"time"
)

var (
	Log          = logging.MustGetLogger("admin")
	MsgSvrClient *rpc.Client
)

const (
	httpReadTimeout = 30 //seconds
)

func main() {
	var err error
	signalCH := InitSignal()

	// Load config
	Conf, err = NewConfig(ConfFile)
	if err != nil {
		panic(err)
	}

	// Set max routine
	runtime.GOMAXPROCS(Conf.MaxProc)

	// Initialize message server client
	if err := InitMsgSvrClient(); err != nil {
		Log.Error("InitMsgSvrClient() failed(%v)", err)
		os.Exit(-1)
	}

	runAdmin()

	Log.Info("Web service start")
	HandleSignal(signalCH)

	// Clost message service client
	MsgSvrClose()

}

func runAdmin() {
	go func() {
		adminServeMux := http.NewServeMux()

		adminServeMux.HandleFunc("/admin/push", AdminPush)

		err := http.ListenAndServe(Conf.AdminAddr, adminServeMux)
		if err != nil {
			Log.Error("http.ListenAndServe(\"%s\") failed(%v)", Conf.AdminAddr, err)
			os.Exit(-1)
		}
	}()
}

// InitMsgSvrClient initialize message service client
func InitMsgSvrClient() error {
	go func() {
		failed := false
		// If process exit, then close Message rpc
		defer func() {
			if MsgSvrClient != nil {
				if err := MsgSvrClient.Close(); err != nil {
					Log.Error("MsgSvrClient.Close() error(%v)", err)
				}
			}
		}()
		for {
			if !failed && MsgSvrClient != nil {
				reply := 0
				if err := MsgSvrClient.Call("MessageRPC.Ping", 0, &reply); err != nil {
					Log.Error("rpc.Call(\"MessageRPC.Ping\")  error(%v)", err)
					failed = true
				} else {
					// every one second send a heartbeat ping
					failed = false
					Log.Debug("rpc ping ok")
					time.Sleep(Conf.MsgPing)
					continue
				}
			}
			// reconnect(init) message rpc
			rpcTmp, err := rpc.Dial("tcp", Conf.MsgAddr)
			if err != nil {
				Log.Error("rpc.Dial(\"tcp\", \"%s\") error(%v), reconnect retry after \"%d\" second", Conf.MsgAddr, err, int64(Conf.MsgRetry)/int64(time.Second))
				time.Sleep(Conf.MsgRetry)
				continue
			}
			MsgSvrClient = rpcTmp
			failed = false
			Log.Info("rpc client reconnect \"%s\" ok", Conf.MsgAddr)
		}
	}()

	return nil
}

// MsgSvrClose close message service client
func MsgSvrClose() {
	if MsgSvrClient != nil {
		if err := MsgSvrClient.Close(); err != nil {
			Log.Error("MsgSvrClient.Close() error(%v)", err)
		}
	}
}

// AdminPush handle for push private message
func AdminPush(rw http.ResponseWriter, r *http.Request) {

}
