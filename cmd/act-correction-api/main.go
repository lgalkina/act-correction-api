package main

import (
	"github.com/lgalkina/act-correction-api/internal/app/retranslator"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	sigs := make(chan os.Signal, 1)

	cfg := retranslator.Config{
		ChannelSize:    512,
		ConsumerCount:  2,
		ConsumeSize:    10,
		ProducerCount:  28,
		WorkerCount:    2,
		ConsumeTimeout: time.Millisecond,
	}

	retranslator, err := retranslator.NewRetranslator(cfg)
	if err != nil {
		log.Fatalf("Error while creating retranslator: %v", err)
	}

	retranslator.Start()

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs
	retranslator.Close()
}
