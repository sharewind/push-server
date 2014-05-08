package worker

type workerOptions struct {
	WorkerHTTPAddress string   `flag:"worker-http-address"`
	BrokerTcpAddress  []string `flag:"broker-tcp-address"`
}

func NewWorkerOptions() *workerOptions {
	return &workerOptions{
		WorkerHTTPAddress: "0.0.0.0:8710",
		// BrokerTcpAddress: "0.0.0.0:8600",
	}
}
