package broker

import (
	"crypto/md5"
	"hash/crc32"
	"io"
	"os"
	"time"
)

type brokerOptions struct {
	// basic options
	ID         int64  `flag:"worker-id" cfg:"id"`
	Verbose    bool   `flag:"verbose"`
	TCPAddress string `flag:"tcp-address"`
	// HTTPAddress      string `flag:"http-address"`
	BroadcastAddress string `flag:"broadcast-address"`

	MaxBodySize   int64 `flag:"max-body-size"`
	ClientTimeout time.Duration

	// TLS config
	TLSCert string `flag:"tls-cert"`
	TLSKey  string `flag:"tls-key"`

	// compression
	DeflateEnabled  bool `flag:"deflate"`
	MaxDeflateLevel int  `flag:"max-deflate-level"`
	SnappyEnabled   bool `flag:"snappy"`
}

func NewBrokerOptions() *brokerOptions {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	o := &brokerOptions{
		TCPAddress: "0.0.0.0:8600",
		// HTTPAddress: "0.0.0.0:8601",

		MaxBodySize:   5 * 1024768,
		ClientTimeout: 60 * time.Second,

		DeflateEnabled:  true,
		MaxDeflateLevel: 6,
		SnappyEnabled:   true,
	}

	h := md5.New()
	io.WriteString(h, hostname)
	o.ID = int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)

	return o
}
