package main

type adminOptions struct {
	HTTPAddress string `flag:"http-address"`
}

func NewAdminOptions() *adminOptions {
	return &adminOptions{
		HTTPAddress: "0.0.0.0:4172",
	}
}
