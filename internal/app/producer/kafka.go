package producer

import (
	"context"
	"github.com/lgalkina/act-correction-api/internal/app/cleaner"
	"github.com/lgalkina/act-correction-api/internal/app/updater"
	"github.com/lgalkina/act-correction-api/internal/model"
	"log"
	"sync"
	"time"

	"github.com/lgalkina/act-correction-api/internal/app/sender"
)

type Producer interface {
	Start(ctx context.Context)
	Close()
}

type producer struct {
	n       uint64
	timeout time.Duration

	sender sender.EventSender
	events <-chan model.CorrectionEvent

	updater updater.Updater
	cleaner cleaner.Cleaner

	wg   *sync.WaitGroup
}

func NewKafkaProducer(
	n uint64,
	sender sender.EventSender,
	events <-chan model.CorrectionEvent,
	updater updater.Updater,
	cleaner cleaner.Cleaner,
) Producer {

	wg := &sync.WaitGroup{}

	return &producer{
		n:          n,
		sender:     sender,
		events:     events,
		wg:         wg,
		updater: updater,
		cleaner: cleaner,
	}
}

func (p *producer) Start(ctx context.Context) {
	p.cleaner.Start(ctx)
	p.updater.Start(ctx)
	for i := uint64(0); i < p.n; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for {
				select {
				case event, ok := <-p.events:
					if ok {
						if err := p.sender.Send(&event); err != nil {
							log.Printf("Error while sending event with ID = %d to Kafka: %v\n", event.ID, err)
							p.updater.Add(event.ID)
						} else {
							log.Printf("Event with ID = %d was successfully sent to Kafka\n", event.ID)
							p.cleaner.Add(event.ID)
						}
					}
				case <- ctx.Done():
					return
				}
			}
		}()
	}
}

func (p *producer) Close() {
	p.cleaner.Close()
	p.updater.Close()
	p.wg.Wait()
}
