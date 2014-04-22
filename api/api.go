package api

import (
	"github.com/op/go-logging"
	"net"
	"runtime"
	"time"

	"code.sohuno.com/kzapp/push-server/util"
)

var log = logging.MustGetLogger("api")

type PushAPI struct {
	httpAddr     *net.TCPAddr
	httpListener net.Listener
	idChan       chan int64
	exitChan     chan int
	waitGroup    util.WaitGroupWrapper
	IdSeq        int64
}

// NewWriter returns an instance of Writer for the specified address
func NewPushAPI(httpAddress *string) *PushAPI {

	if len(*httpAddress) == 0 {
		log.Fatalf("httpAddress required.")
	}

	httpAddr, err := net.ResolveTCPAddr("tcp", *httpAddress)
	if err != nil {
		log.Fatal(err)
	}

	p := &PushAPI{
		httpAddr: httpAddr,
		idChan:   make(chan int64, 4096),
		exitChan: make(chan int),
	}
	return p
}

func (p *PushAPI) Main() {
	context := &context{p}

	httpListener, err := net.Listen("tcp", p.httpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", p.httpAddr, err.Error())
	}
	p.httpListener = httpListener
	httpServer := &httpServer{context: context}
	p.waitGroup.Wrap(func() { util.HTTPServer(p.httpListener, httpServer) })
	p.waitGroup.Wrap(func() { p.idPump() })
}

func (p *PushAPI) idPump() {
	factory := &util.GuidFactory{}
	lastError := time.Now()
	for {
		id, err := factory.NewGUID(p.IdSeq)
		if err != nil {
			now := time.Now()
			if now.Sub(lastError) > time.Second {
				// only print the error once/second
				log.Error("%s", err.Error())
				lastError = now
			}
			runtime.Gosched()
			continue
		}
		select {
		case p.idChan <- id:
		case <-p.exitChan:
			goto exit
		}
	}

exit:
	log.Debug("ID Pump: closing")
}

func (p *PushAPI) Exit() {
	p.httpListener.Close()
	close(p.exitChan)
	close(p.idChan)
	p.waitGroup.Wait()
}
