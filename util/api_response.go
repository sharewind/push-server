package util

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

func ApiResponse(w http.ResponseWriter, statusCode int, errorCode int, errorMsg string, data interface{}) {
	response, err := json.Marshal(struct {
		Code int         `json:"code"`
		Msg  string      `json:"msg"`
		Data interface{} `json:"data"`
	}{
		errorCode,
		errorMsg,
		data,
	})
	if err != nil {
		response = []byte(fmt.Sprintf(`{"status_code":500, "status_txt":"%s", "data":null}`, err.Error()))
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(response)))
	w.WriteHeader(statusCode)

	w.Write(response)
}
