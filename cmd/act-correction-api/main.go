package main

import (
	"context"
	"github.com/lgalkina/act-correction-api/internal/app/retranslator"
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

	retranslator := retranslator.NewRetranslator(cfg)

	retranslator.Start(context.Background())

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs
	retranslator.Close()
}
