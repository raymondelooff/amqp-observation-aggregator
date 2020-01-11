package main

import (
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/raymondelooff/amqp-observation-aggregator/aggregator"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("error: config file location not specified")
	}

	f, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	c := aggregator.Config{}
	err = yaml.Unmarshal(f, &c)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	db, err := aggregator.NewDbConnection(c.MySQL)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	// Set up logger
	var logger *zap.Logger
	if c.Env == "dev" {
		logger, err = zap.NewDevelopment()
	} else {
		logger, err = zap.NewProduction()
	}
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	defer logger.Sync()
	sugar := logger.Sugar()

	// Set up aggregator
	a := aggregator.NewAggregator(c, db, sugar)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		exit := make(chan os.Signal, 1)
		signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)

		<-exit

		sugar.Info("aggregator: shutting down")
		wg.Done()
	}()

	a.Run(&wg)
	sugar.Info("aggregator: shutdown OK")
}
