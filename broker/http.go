package broker

import (
	"io"
	"net/http"

	// "code.sohuno.com/kzapp/push-server/util"
)

import httpprof "net/http/pprof"

type httpServer struct {
	context *context
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.URL.Path {
	case "/ping":
		s.pingHandler(w, req)
	case "/info":
		s.infoHandler(w, req)
	case "/stat":
		s.statsHandler(w, req)
	case "/debug/pprof":
		httpprof.Index(w, req)
	case "/debug/pprof/cmdline":
		httpprof.Cmdline(w, req)
	case "/debug/pprof/symbol":
		httpprof.Symbol(w, req)
	case "/debug/pprof/heap":
		httpprof.Handler("heap").ServeHTTP(w, req)
	case "/debug/pprof/goroutine":
		httpprof.Handler("goroutine").ServeHTTP(w, req)
	case "/debug/pprof/profile":
		httpprof.Profile(w, req)
	case "/debug/pprof/block":
		httpprof.Handler("block").ServeHTTP(w, req)
	case "/debug/pprof/threadcreate":
		httpprof.Handler("threadcreate").ServeHTTP(w, req)
	default:
		// log.Printf("ERROR: 404 %s", req.URL.Path)
		// util.ApiResponse(w, 404, "NOT_FOUND", nil)
	}
}

func (s *httpServer) pingHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func (s *httpServer) infoHandler(w http.ResponseWriter, req *http.Request) {
	// util.ApiResponse(w, 200, "OK", struct {
	// 	Version string `json:"version"`
	// }{
	// 	Version: util.BINARY_VERSION,
	// })
}

func (s *httpServer) statsHandler(w http.ResponseWriter, req *http.Request) {
	io.WriteString(w, s.context.broker.GetStats())
}
