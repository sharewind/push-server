package worker

import (
	// "bufio"
	// "errors"
	// "fmt"
	"io"
	"io/ioutil"
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

const (
	NotFound      = -1
	OK            = 0
	ParamErr      = 1
	InternalErr   = 2
	MethodErr     = 3
	MsgEmpty      = 4
	MsgTooLong    = 5
	MsgErr        = 6
	ChannelIdErr  = 7
	DeviceTypeErr = 8
	NoSubErr      = 9
)

var (
	Msg map[int]string
)

func init() {
	// Err massage
	Msg = make(map[int]string)

	Msg[NotFound] = "not found"
	Msg[OK] = "OK"
	Msg[ParamErr] = "param error"
	Msg[InternalErr] = "internal exception"
	Msg[MethodErr] = "method error"
	Msg[MsgEmpty] = "message is empty"
	Msg[MsgTooLong] = "message is too long"
	Msg[MsgErr] = "message is error"
	Msg[ChannelIdErr] = "channel id is error"
	Msg[DeviceTypeErr] = "device type is error"
	Msg[NoSubErr] = "channel has no sub"
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.URL.Path {
	case "/pub":
		fallthrough
	case "/put":
		s.putHandler(w, req)
	case "/create_channel":
		s.createChannelHandler(w, req)
	case "/update_channel":
		// do nothing
	case "/delete_channel":
		// do nothing
	case "/get_channel/":
		// do nothing
	// case "/stats":
	// s.statsHandler(w, req)
	case "/ping":
		s.pingHandler(w, req)
	case "/info":
		s.infoHandler(w, req)
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
		log.Error("404 %s", req.URL.Path)
		util.ApiResponse(w, 404, NotFound, Msg[NotFound], nil)
	}
}

func (s *httpServer) pingHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func (s *httpServer) infoHandler(w http.ResponseWriter, req *http.Request) {
	util.ApiResponse(w, 200, OK, Msg[OK], struct {
		Version string `json:"version"`
	}{
		Version: util.BINARY_VERSION,
	})
}

// curl -X POST /create_channel?key=10000001&id=1001&name=test&creator=kuaizhan_user_name&app_id=1001&app_name=test
func (s *httpServer) createChannelHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		util.ApiResponse(w, 400, MethodErr, Msg[MethodErr], nil)
		return
	}

	reqParams, err := util.NewReqParams(req)
	if err != nil {
		util.ApiResponse(w, 400, ParamErr, Msg[ParamErr], nil)
		return
	}

	//check auth key
	key, err := reqParams.Get("key")
	if err != nil {
		util.ApiResponse(w, 400, ParamErr, Msg[ParamErr], nil)
		return
	}
	if key != "10000001" {
		util.ApiResponse(w, 400, ParamErr, Msg[ParamErr], nil)
		return
	}

	idStr, err := reqParams.Get("id")
	if err != nil {
		util.ApiResponse(w, 400, ParamErr, Msg[ParamErr], nil)
		return
	}

	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		util.ApiResponse(w, 400, ParamErr, Msg[ParamErr], nil)
		return
	}

	name, err := reqParams.Get("name")
	if err != nil {
		util.ApiResponse(w, 400, ParamErr, Msg[ParamErr], nil)
		return
	}

	creatorStr, err := reqParams.Get("creator")
	if err != nil {
		util.ApiResponse(w, 400, ParamErr, Msg[ParamErr], nil)
		return
	}

	appidStr, err := reqParams.Get("app_id")
	if err != nil {
		util.ApiResponse(w, 400, ParamErr, Msg[ParamErr], nil)
		return
	}

	appid, err := strconv.ParseInt(appidStr, 10, 64)
	if err != nil {
		util.ApiResponse(w, 400, ParamErr, Msg[ParamErr], nil)
		return
	}

	appname, err := reqParams.Get("app_name")
	if err != nil {
		util.ApiResponse(w, 400, ParamErr, Msg[ParamErr], nil)
		return
	}

	channel := &model.Channel{id, name, time.Now().UnixNano(), creatorStr, appid, appname, 0}
	err = model.SaveChannel(channel)
	if err != nil {
		util.ApiResponse(w, 400, InternalErr, Msg[InternalErr], nil)
		return
	}

	util.ApiResponse(w, 200, OK, Msg[OK], nil)
}

//curl -d "hi sucess " http://localhost:8710/put?channel_id=1001&device_type=0
func (s *httpServer) putHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		util.ApiResponse(w, 400, MethodErr, Msg[MethodErr], nil)
		return
	}

	// TODO: one day I'd really like to just error on chunked requests
	// to be able to fail "too big" requests before we even read

	MaxMsgSize := int64(1024)
	if req.ContentLength > MaxMsgSize {
		util.ApiResponse(w, 400, MsgTooLong, Msg[MsgTooLong], nil)
		return
	}

	// // add 1 so that it's greater than our max when we test for it
	// // (LimitReader returns a "fake" EOF)
	// readMax := s.context.nsqd.options.MaxMsgSize + 1
	readMax := MaxMsgSize + 1
	body, err := ioutil.ReadAll(io.LimitReader(req.Body, readMax))
	if err != nil {
		util.ApiResponse(w, 400, MsgErr, Msg[MsgErr], nil)
		return
	}
	if int64(len(body)) == readMax {
		log.Error("/put hit max message size")
		util.ApiResponse(w, 400, MsgErr, Msg[MsgErr], nil)
		return
	}
	if len(body) == 0 {
		util.ApiResponse(w, 400, MsgEmpty, Msg[MsgEmpty], nil)
		return
	}

	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		log.Error("failed to parse request params - %s", err.Error())
		util.ApiResponse(w, 400, ParamErr, Msg[ParamErr], nil)
		return
	}

	channel_id, err := strconv.ParseInt(reqParams.Get("channel_id"), 10, 64)
	if err != nil {
		log.Error("failed to parse channel_id params - %s", err.Error())
		util.ApiResponse(w, 400, ChannelIdErr, Msg[ChannelIdErr], nil)
		return
	}
	_, err = model.FindChannelByID(channel_id)
	if err != nil {
		log.Error("can not find channel  - %s", err.Error())
		util.ApiResponse(w, 400, NoSubErr, Msg[NoSubErr], nil)
		return
	}

	// push_type, err := strconv.ParseInt(reqParams.Get("push_type"), 10, 8)
	// if err != nil {
	// 	log.Error("failed to parse push_type params - %s", err.Error())
	// 	util.ApiResponse(w, 400, "INVALID_REQUEST", nil)
	// 	return
	// }

	device_type, err := strconv.ParseInt(reqParams.Get("device_type"), 10, 8)
	if err != nil {
		log.Error("failed to parse device_type params - %s", err.Error())
		util.ApiResponse(w, 400, DeviceTypeErr, Msg[DeviceTypeErr], nil)
		return
	}

	msg := &model.Message{
		ID:        <-s.context.worker.idChan,
		ChannelID: channel_id,
		CreatedAt: time.Now().UnixNano(),
		Body:      string(body),
		// PushType:   int8(push_type),
		DeviceType: int8(device_type),
	}
	err = model.SaveMessage(msg)
	if err != nil {
		log.Error("failed to SaveMessage %#v ,err=%s", msg, err.Error())
		util.ApiResponse(w, 500, InternalErr, Msg[InternalErr], nil)
		return
	}

	err = s.context.worker.PutMessage(msg)
	if err != nil {
		util.ApiResponse(w, 500, InternalErr, Msg[InternalErr], nil)
		return
	}

	util.ApiResponse(w, 200, OK, Msg[OK], nil)
	return
}
