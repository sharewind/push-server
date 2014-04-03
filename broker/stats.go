package broker

type ClientStats struct {
	// TODO: deprecated, remove in 1.0
	Name string `json:"name"`

	ClientID      string `json:"client_id"`
	Hostname      string `json:"hostname"`
	Version       string `json:"version"`
	RemoteAddress string `json:"remote_address"`
	State         int32  `json:"state"`
	ReadyCount    int64  `json:"ready_count"`
	InFlightCount int64  `json:"in_flight_count"`
	MessageCount  uint64 `json:"message_count"`
	FinishCount   uint64 `json:"finish_count"`
	RequeueCount  uint64 `json:"requeue_count"`
	ConnectTime   int64  `json:"connect_ts"`
	SampleRate    int32  `json:"sample_rate"`
	TLS           bool   `json:"tls"`
	Deflate       bool   `json:"deflate"`
	Snappy        bool   `json:"snappy"`
	UserAgent     string `json:"user_agent"`
}
