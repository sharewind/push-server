package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	// "io/ioutil"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"code.sohuno.com/kzapp/push-server/admin/templates"
	"code.sohuno.com/kzapp/push-server/model"
	"code.sohuno.com/kzapp/push-server/util"
	// "github.com/bitly/go-nsq"
)

// this is similar to httputil.NewSingleHostReverseProxy except it passes along basic auth
func NewSingleHostReverseProxy(target *url.URL, timeout time.Duration) *httputil.ReverseProxy {
	director := func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		if target.User != nil {
			passwd, _ := target.User.Password()
			req.SetBasicAuth(target.User.Username(), passwd)
		}
	}
	return &httputil.ReverseProxy{
		Director:  director,
		Transport: util.NewDeadlineTransport(timeout),
	}
}

type httpServer struct {
	context  *Context
	counters map[string]map[string]int64
	proxy    *httputil.ReverseProxy
}

func NewHTTPServer(context *Context) *httpServer {
	var proxy *httputil.ReverseProxy

	templates.T.Funcs(template.FuncMap{
		"commafy":        util.Commafy,
		"nanotohuman":    util.NanoSecondToHuman,
		"floatToPercent": util.FloatToPercent,
		"percSuffix":     util.PercSuffix,
	})
	templates.Parse()

	return &httpServer{
		context:  context,
		counters: make(map[string]map[string]int64),
		proxy:    proxy,
	}
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// if strings.HasPrefix(req.URL.Path, "/node/") {
	// 	s.nodeHandler(w, req)
	// 	return
	// } else
	if strings.HasPrefix(req.URL.Path, "/channel/") {
		s.channelHandler(w, req)
		return
	}

	switch req.URL.Path {
	case "/message":
		s.messageHandler(w, req)
	case "/channel":
		s.channelHandler(w, req)

	case "/message_list":
		s.messageListHandler(w, req)
	case "/channel_list":
		s.channelListHandler(w, req)
	case "/device_list":
		s.deviceListHandler(w, req)
	case "/sub_list":
		s.subListHandler(w, req)
	case "/sub_count":
		s.subCountHandler(w, req)

	case "/":
		s.indexHandler(w, req)
	default:
		log.Debug("ERROR: 404 %s", req.URL.Path)
		http.NotFound(w, req)
	}
}

func (s *httpServer) messageHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Debug("ERROR: failed to parse request params - %s", err.Error())
		http.Error(w, "INVALID_REQUEST", 500)
		return
	}
	ID, err := reqParams.Get("ID")
	if err != nil {
		log.Error(err.Error())
		http.Error(w, err.Error(), 500)
		return
	}
	id, _ := strconv.ParseInt(ID, 10, 64)
	result, err := model.FindMessageByID(id)
	if err != nil {

		log.Error(err.Error())
		http.Error(w, err.Error(), 500)
		return
	}
	r, _ := json.Marshal(result)
	io.WriteString(w, string(r))
}

func (s *httpServer) messageListHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, err.Error(), 500)
		return
	}

	idStr, err := reqParams.Get("channel_id")
	if err != nil {
		log.Error(err.Error())
		http.Error(w, err.Error(), 500)
		return
	}

	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, err.Error(), 500)
		return
	}
	skip, limit := GetSkipLimit(reqParams)
	messages, err := model.GetMessageByChannelId(id, skip, limit)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, err.Error(), 500)
		return
	}

	for i := 0; i < len(*messages); i++ {
		(*messages)[i].OK, (*messages)[i].Err = model.GetMsgOKErrCount((*messages)[i].ID)
	}

	// for _, m := range *result {
	// 	m.OK, m.Err, err = model.GetMsgOKErrCount(m.ID)
	// 	if err != nil {
	// 		log.Error("msg ok err count is error: %s", err.Error())
	// 	}
	// }

	r, err := json.Marshal(messages)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, err.Error(), 500)
		return
	}
	log.Debug(string(r))
	io.WriteString(w, string(r))
	return

}

func (s *httpServer) channelListHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, err.Error(), 500)
		return
	}
	result, err := model.ListChannel(GetSkipLimit(reqParams))
	if err != nil {
		log.Error(err.Error())
		http.Error(w, err.Error(), 500)
		return
	}
	r, _ := json.Marshal(result)
	io.WriteString(w, string(r))
}

func (s *httpServer) deviceListHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, err.Error(), 500)
		return
	}
	result, err := model.ListDevice(GetSkipLimit(reqParams))
	if err != nil {
		log.Error(err.Error())
		http.Error(w, err.Error(), 500)
		return
	}
	r, _ := json.Marshal(result)
	io.WriteString(w, string(r))
}

func (s *httpServer) subListHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, err.Error(), 500)
		return
	}
	result, err := model.ListSubscribe(GetSkipLimit(reqParams))
	if err != nil {
		log.Error(err.Error())
		http.Error(w, err.Error(), 500)
		return
	}
	r, _ := json.Marshal(result)
	io.WriteString(w, string(r))
}

func (s *httpServer) subCountHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, _ := util.NewReqParams(req)
	channelId, _ := reqParams.Get("channel_id")
	id, _ := strconv.ParseInt(channelId, 10, 64)
	result, err := model.CountSubscribe(id, 0)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, err.Error(), 500)
		return
	}
	r, _ := json.Marshal(result)
	io.WriteString(w, string(r))
}

func (s *httpServer) indexHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, err.Error(), 500)
		return
	}
	channels, err := model.ListChannel(GetSkipLimit(reqParams))

	p := struct {
		Title    string
		Channels *[]model.Channel
		Version  string
	}{
		Title:    "push",
		Channels: channels,
		Version:  util.BINARY_VERSION,
	}
	err = templates.T.ExecuteTemplate(w, "index.html", p)
	if err != nil {
		log.Debug("Template Error %s", err.Error())
		http.Error(w, "Template Error", 500)
	}
}

func (s *httpServer) channelHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method == "POST" {
		reqParams, err := util.NewReqParams(req)
		if err != nil {
			log.Debug("ERROR: failed to parse request params - %s", err.Error())
			http.Error(w, "INVALID_REQUEST", 500)
			return
		}

		//check auth key
		key, err := reqParams.Get("key")
		if err != nil {
			log.Error(err.Error())
			http.Error(w, err.Error(), 500)
			return
		}
		if key != "10000001" {
			log.Debug("key %s error", key)
			http.Error(w, "key error", 500)
		}

		idStr, err := reqParams.Get("id")
		if err != nil {
			log.Error(err.Error())
			http.Error(w, err.Error(), 500)
			return
		}
		id, _ := strconv.ParseInt(idStr, 10, 64)

		name, err := reqParams.Get("name")
		if err != nil {
			log.Error(err.Error())
			http.Error(w, err.Error(), 500)
			return
		}

		creatorStr, err := reqParams.Get("creator")
		if err != nil {
			log.Error(err.Error())
			http.Error(w, err.Error(), 500)
			return
		}

		appidStr, err := reqParams.Get("appid")
		if err != nil {
			log.Error(err.Error())
			http.Error(w, err.Error(), 500)
			return
		}
		appid, _ := strconv.ParseInt(appidStr, 10, 64)

		appname, err := reqParams.Get("appname")
		if err != nil {
			log.Error(err.Error())
			http.Error(w, err.Error(), 500)
			return
		}

		channel := &model.Channel{id, name, time.Now().UnixNano(), creatorStr, appid, appname, 0}
		err = model.SaveChannel(channel)
		if err != nil {
			log.Error(err.Error())
			http.Error(w, err.Error(), 500)
			return
		}

		r, _ := json.Marshal(channel)
		io.WriteString(w, string(r))

	} else {

		var urlRegex = regexp.MustCompile(`^/channel/(.*)$`)
		matches := urlRegex.FindStringSubmatch(req.URL.Path)
		if len(matches) == 0 {
			http.Error(w, "INVALID_CHANNEL", 500)
			return
		}
		parts := strings.Split(matches[1], "/")
		channelId, _ := strconv.ParseInt(parts[0], 10, 64)
		channel, err := model.FindChannelByID(channelId)
		if err != nil {
			http.Error(w, "INVALID_CHANNEL", 500)
			return
		}

		messages, err := model.GetMessageByChannelId(channel.ID, 0, 10)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		for i := 0; i < len(*messages); i++ {
			(*messages)[i].OK, (*messages)[i].Err = model.GetMsgOKErrCount((*messages)[i].ID)
		}

		// for _, m := range *messages {
		// 	m.OK, m.Err, err = model.GetMsgOKErrCount(m.ID)
		// 	if err != nil {
		// 		log.Error(err.Error())
		// 	}
		// 	// log.Debug("mid:%d mcreateat%d\n", m.ID, m.CreatedAt)
		// }

		subs, err := model.GetSubscribeByChannelId(channel.ID, 0, 10)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		subCount := make(map[string]int)
		subCount["all_device"], err = model.CountSubscribeByChannelId(channel.ID, model.ALLDevice)
		subCount["android"], err = model.CountSubscribeByChannelId(channel.ID, model.Android)
		subCount["ios"], err = model.CountSubscribeByChannelId(channel.ID, model.IOS)
		subCount["browser"], err = model.CountSubscribeByChannelId(channel.ID, model.Browser)
		subCount["PC"], err = model.CountSubscribeByChannelId(channel.ID, model.PC)
		subCount["windowsphone"], err = model.CountSubscribeByChannelId(channel.ID, model.WindowsPhone)
		subCount["other"], err = model.CountSubscribeByChannelId(channel.ID, model.Other)

		p := struct {
			Title      string
			Version    string
			Topic      string
			Channel    *model.Channel
			Messages   *[]model.Message
			Subscribes *[]model.Subscribe
			SubCount   map[string]int
		}{
			Title:      fmt.Sprintf("push %s", channel.Name),
			Version:    util.BINARY_VERSION,
			Topic:      channel.Name,
			Channel:    channel,
			Messages:   messages,
			Subscribes: subs,
			SubCount:   subCount,
		}
		err = templates.T.ExecuteTemplate(w, "channel.html", p)
		if err != nil {
			log.Debug("Template Error %s", err.Error())
			http.Error(w, "Template Error", 500)
		}
	}
}

func GetSkipLimit(params *util.ReqParams) (skip int, limit int) {
	skipStr, err := params.Get("skip")
	if err != nil {
		log.Error(err.Error())
		skip = 0
	}
	skip, err = strconv.Atoi(skipStr)
	if err != nil || skip < 0 {
		log.Error(err.Error())
		skip = 0
	}
	limitStr, err := params.Get("limit")
	if err != nil {
		log.Error(err.Error())
		limit = 10
	}
	limit, err = strconv.Atoi(limitStr)
	if err != nil || limit < 0 || limit > 100 {
		log.Error(err.Error())
		limit = 10
	}
	return
}
