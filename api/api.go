package api

import (
	"crypto/md5"
	"github.com/op/go-logging"
	"hash/crc32"
	"io"
	"net"
	"os"
	"runtime"
	"time"

	"code.sohuno.com/kzapp/push-server/util"
)

var module = "api"
var log = logging.MustGetLogger(module)

type PushAPI struct {
	httpAddr      *net.TCPAddr
	brokerTcpAddr *string
	httpListener  net.Listener
	idChan        chan int64
	exitChan      chan int
	waitGroup     util.WaitGroupWrapper
	IDSeq         int64
	logLevel      *string
}

// NewWriter returns an instance of Writer for the specified address
func NewPushAPI(httpAddress *string, brokerTcpAddress *string, logLevel *string) *PushAPI {

	if len(*httpAddress) == 0 {
		log.Fatalf("httpAddress required.")
	}

	httpAddr, err := net.ResolveTCPAddr("tcp", *httpAddress)
	if err != nil {
		log.Fatal(err)
	}

	hostname, err := os.Hostname()
	//hostname := "sohu"
	if err != nil {
		log.Fatal(err)
	}
	h := md5.New()
	io.WriteString(h, hostname)
	ID := int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)

	p := &PushAPI{
		httpAddr:      httpAddr,
		brokerTcpAddr: brokerTcpAddress,
		idChan:        make(chan int64, 4096),
		exitChan:      make(chan int),
		IDSeq:         ID,
		logLevel:      logLevel,
	}
	return p
}

func (p *PushAPI) Main() {

	util.SetLevel(module, p.logLevel)

	context := &context{p}

	httpListener, err := net.Listen("tcp", p.httpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", p.httpAddr, err.Error())
	}
	p.httpListener = httpListener
	httpServer := &httpServer{context: context}

	for i := 0; i < 4; i++ {
		p.waitGroup.Wrap(func() { util.HTTPServer(p.httpListener, httpServer) })
	}
	p.waitGroup.Wrap(func() { p.idPump() })
}

func (p *PushAPI) idPump() {
	factory := &util.GuidFactory{}
	lastError := time.Now()
	for {
		id, err := factory.NewGUID(p.IDSeq)
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
