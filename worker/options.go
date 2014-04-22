package worker

type workerOptions struct {
	HTTPAddress      string   `flag:"http-address"`
	BrokerTcpAddress []string `flag:"broker-tcp-address"`
}

func NewWorkerOptions() *workerOptions {
	return &workerOptions{
	// HTTPAddress: "0.0.0.0:8710",
	// BrokerTcpAddress: "0.0.0.0:8600",
	}
}
