package main

import (
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/raymondelooff/amqp-observation-aggregator/aggregator"
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

	a := aggregator.NewAggregator(c, db)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		exit := make(chan os.Signal, 1)
		signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)

		<-exit

		log.Println("Aggregator: shutting down")
		wg.Done()
	}()

	a.Run(&wg)
	log.Println("Aggregator: shutdown OK")
}
