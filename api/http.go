package api

import (
	// "bufio"
	// "errors"
	// "fmt"
	"io"
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

// curl -X POST http://localhost:4171/registration?serial_no=SOHUNO20140401XX&device_type=3&device_name=搜狐Android测试机
func (s *httpServer) registerHandler(w http.ResponseWriter, req *http.Request) {
	log.Printf("INFO: request %s ", req.URL.RawQuery)

	if req.Method != "POST" {
		util.ApiResponse(w, 405, "METHOD_NOT_SUPPORT", nil)
		return
	}

	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		util.ApiResponse(w, 400, "INVALID_REQUEST", nil)
		return
	}

	serial_no := reqParams.Get("serial_no")
	if len(serial_no) == 0 {
		util.ApiResponse(w, 400, "serial_no is required", nil)
		return
	}

	device_name := reqParams.Get("device_name")
	device_type, err := strconv.ParseInt(reqParams.Get("device_type"), 10, 8)
	if device_type != model.Android {
		util.ApiResponse(w, 400, "INVALID_DEVICE_TYPE", nil)
		return
	}

	var device *model.Device = nil
	deviceID, err := model.FindDeviceIDBySerialNO(serial_no)
	log.Printf("INFO: FindDeviceIDBySerialNO %s result %s", serial_no, deviceID)

	if err == nil && deviceID != 0 {
		device, err = model.FindDeviceByID(deviceID)
		log.Printf("INFO: FindDeviceByID %s result %#v", deviceID, device)

		if err != nil || device == nil {
			log.Printf("ERROR: FindDeviceByID error %s", err)
			util.ApiResponse(w, 500, "INTERNAL_ERROR", nil)
			return
		}
	}

	log.Printf("INFO: exist_device %#v", device)
	if device == nil {
		device = &model.Device{
			ID:              <-s.context.api.idChan,
			SerialNO:        serial_no,
			DeviceName:      device_name,
			DeviceType:      int8(device_type),
			CreatedAt:       time.Now().UnixNano(),
			OnlineTimestamp: time.Now().UnixNano(),
		}
		err = model.SaveDevice(device)
		if err != nil {
			log.Printf("ERROR: SaveDevice %s error %s", device, err)
			util.ApiResponse(w, 500, "INTERNAL_ERROR", nil)
			return
		}
	}
	data := make(map[string]interface{})
	data["broker"] = "b1.zhan.sohu.com"
	data["device_id"] = device.ID
	util.ApiResponse(w, 200, "OK", data)
}
