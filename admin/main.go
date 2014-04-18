package main

import (
	"flag"
	"fmt"
	"github.com/op/go-logging"
	"os"
	"os/signal"
	"syscall"

	"code.sohuno.com/kzapp/push-server/util"
	"github.com/BurntSushi/toml"
	"github.com/mreiferson/go-options"
)

var (
	log     = logging.MustGetLogger("main")
	flagSet = flag.NewFlagSet("admin", flag.ExitOnError)

	config      = flagSet.String("config", "./admin.cfg", "path to config file")
	showVersion = flagSet.Bool("version", false, "print version string")

	httpAddress = flagSet.String("http-address", "", "<addr>:<port> to listen on for HTTP clients")
	templateDir = flagSet.String("template-dir", "", "path to templates directory")
)

func main() {
	flagSet.Parse(os.Args[1:])

	if *showVersion {
		fmt.Println(util.Version("admin"))
		return
	}

	if *templateDir != "" {
		log.Debug("WARNING: --template-dir is deprecated and will be removed in the next release (templates are now compiled into the binary)")
	}

	exitChan := make(chan int)
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		exitChan <- 1
	}()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	var cfg map[string]interface{}
	if *config != "" {
		_, err := toml.DecodeFile(*config, &cfg)
		if err != nil {
			log.Fatalf("ERROR: failed to load config file %s - %s", *config, err.Error())
		}
	}

	opts := NewAdminOptions()
	options.Resolve(opts, flagSet, cfg)
	admin := Newadmin(opts)

	admin.Main()
	<-exitChan
	admin.Exit()
}
