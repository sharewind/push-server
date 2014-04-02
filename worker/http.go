package worker

import (
	// "bufio"
	// "errors"
	// "fmt"
	"io"
	"io/ioutil"
	"log"
	// "net"
	"net/http"
	"net/url"
	"strconv"
	// "strings"
	"time"

	"code.sohuno.com/kzapp/push-server/model"
	"code.sohuno.com/kzapp/push-server/util"
)

import httpprof "net/http/pprof"

type httpServer struct {
	context *context
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.URL.Path {
	case "/pub":
		fallthrough
	case "/put":
		s.putHandler(w, req)
	// case "/mpub":
	// fallthrough
	// case "/mput":
	// s.mputHandler(w, req)
	// case "/stats":
	// s.statsHandler(w, req)
	case "/ping":
		s.pingHandler(w, req)
	case "/info":
		s.infoHandler(w, req)
	// case "/empty_topic":
	// 	s.emptyTopicHandler(w, req)
	// case "/delete_topic":
	// 	s.deleteTopicHandler(w, req)
	// case "/pause_topic":
	// 	s.pauseTopicHandler(w, req)
	// case "/unpause_topic":
	// 	s.pauseTopicHandler(w, req)
	// case "/empty_channel":
	// 	s.emptyChannelHandler(w, req)
	// case "/delete_channel":
	// 	s.deleteChannelHandler(w, req)
	// case "/pause_channel":
	// 	s.pauseChannelHandler(w, req)
	// case "/unpause_channel":
	// 	s.pauseChannelHandler(w, req)
	// case "/create_topic":
	// 	s.createTopicHandler(w, req)
	// case "/create_channel":
	// 	s.createChannelHandler(w, req)
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
		log.Printf("ERROR: 404 %s", req.URL.Path)
		util.ApiResponse(w, 404, "NOT_FOUND", nil)
	}
}

func (s *httpServer) pingHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func (s *httpServer) infoHandler(w http.ResponseWriter, req *http.Request) {
	util.ApiResponse(w, 200, "OK", struct {
		Version string `json:"version"`
	}{
		Version: util.BINARY_VERSION,
	})
}

// func (s *httpServer) createChannelHandler(w http.ResponseWriter, req *http.Request) {
// 	reqParams, err := util.NewReqParams(req)
// 	if err != nil {
// 		log.Printf("ERROR: failed to parse request params - %s", err.Error())
// 		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
// 		return
// 	}

// 	topicName, channelName, err := util.GetTopicChannelArgs(reqParams)
// 	if err != nil {
// 		util.ApiResponse(w, 500, err.Error(), nil)
// 		return
// 	}

// 	topic, err := s.context.nsqd.GetExistingTopic(topicName)
// 	if err != nil {
// 		util.ApiResponse(w, 500, "INVALID_TOPIC", nil)
// 		return
// 	}

// 	topic.GetChannel(channelName)
// 	util.ApiResponse(w, 200, "OK", nil)
// }

func (s *httpServer) putHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	// TODO: one day I'd really like to just error on chunked requests
	// to be able to fail "too big" requests before we even read

	MaxMsgSize := int64(1024)
	if req.ContentLength > MaxMsgSize {
		util.ApiResponse(w, 500, "MSG_TOO_BIG", nil)
		return
	}

	// // add 1 so that it's greater than our max when we test for it
	// // (LimitReader returns a "fake" EOF)
	// readMax := s.context.nsqd.options.MaxMsgSize + 1
	readMax := MaxMsgSize + 1
	body, err := ioutil.ReadAll(io.LimitReader(req.Body, readMax))
	if err != nil {
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}
	if int64(len(body)) == readMax {
		log.Printf("ERROR: /put hit max message size")
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}
	if len(body) == 0 {
		util.ApiResponse(w, 500, "MSG_EMPTY", nil)
		return
	}

	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	channel_id, _ := strconv.ParseInt(reqParams.Get("channel_id"), 10, 64)
	// device_type = strconv.ParseInt(reqParams["device_type"], 10 , 32)

	msg := &model.Message{
		ID:        <-s.context.worker.idChan,
		ChannelID: channel_id,
		CreatedAt: time.Now().UnixNano(),
		Body:      string(body),
	}

	err = s.context.worker.PutMessage(msg)
	if err != nil {
		util.ApiResponse(w, 500, "NOK", nil)
		return
	}

	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}
