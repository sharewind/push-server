package api

import (
	// "bufio"
	// "errors"
	// "fmt"
	"io"
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
	SerialNoErr   = 9
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
	Msg[SerialNoErr] = "serial NO is error"
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.URL.Path {
	case "/registration":
		s.registerHandler(w, req)
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
		log.Debug("ERROR: 404 %s", req.URL.Path)
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

// curl -X POST http://localhost:4171/registration?serial_no=SOHUNO20140401XX&device_type=3&device_name=搜狐Android测试机
func (s *httpServer) registerHandler(w http.ResponseWriter, req *http.Request) {
	log.Debug("INFO: request %s ", req.URL.RawQuery)

	// if req.Method != "POST" {
	// 	util.ApiResponse(w, 405, "METHOD_NOT_SUPPORT", nil)
	// 	return
	// }

	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		log.Debug("ERROR: failed to parse request params - %s", err.Error())
		util.ApiResponse(w, 400, ParamErr, Msg[ParamErr], nil)
		return
	}

	channel_id, err := strconv.ParseInt(reqParams.Get("channel_id"), 10, 64)
	if err != nil || channel_id == 0 {
		util.ApiResponse(w, 400, ChannelIdErr, Msg[ChannelIdErr], nil)
	}

	serial_no := reqParams.Get("serial_no")
	if len(serial_no) == 0 {
		util.ApiResponse(w, 400, SerialNoErr, Msg[SerialNoErr], nil)
		return
	}

	device_name := reqParams.Get("device_name")
	// device_type, err := strconv.ParseInt(reqParams.Get("device_type"), 10, 8)
	// if device_type != model.Android {
	// 	util.ApiResponse(w, 400, "INVALID_DEVICE_TYPE", nil)
	// 	return
	// }

	result := model.CheckOrCreateChannel(channel_id)
	if result == false {
		log.Debug("CheckOrCreateChannel error")
	} else {
		log.Debug("CheckOrCreateChannel ok")
	}

	var device *model.Device = nil
	deviceID, err := model.FindDeviceIDBySerialNO(serial_no)
	log.Debug("INFO: FindDeviceIDBySerialNO %s result %s", serial_no, deviceID)

	if err == nil && deviceID != 0 {
		device, err = model.FindDeviceByID(deviceID)
		// log.Debug("INFO: FindDeviceByID %d result %#v", deviceID, device)
		if err != nil || device == nil {
			log.Debug("ERROR: FindDeviceByID error %s", err)
			util.ApiResponse(w, 500, InternalErr, Msg[InternalErr], nil)
			return
		}
	}

	// log.Debug("INFO: exist_device %#v", device)
	if device == nil {
		device = &model.Device{
			ID:              <-s.context.api.idChan,
			SerialNO:        serial_no,
			DeviceName:      device_name,
			DeviceType:      int8(3), //int8(device_type),
			CreatedAt:       time.Now().UnixNano(),
			OnlineTimestamp: time.Now().UnixNano(),
		}
		err = model.SaveDevice(device)
		if err != nil {
			log.Debug("ERROR: SaveDevice %s error %s", device, err)
			util.ApiResponse(w, 500, InternalErr, Msg[InternalErr], nil)
			return
		}
	} else {

	}
	data := make(map[string]interface{})
	data["broker"] = "b1.zhan.sohu.com"
	data["device_id"] = device.ID
	log.Info("INFO: regiest success %s", serial_no)
	util.ApiResponse(w, 200, OK, Msg[OK], data)
}
