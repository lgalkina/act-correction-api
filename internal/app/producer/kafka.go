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

	"github.com/gammazero/workerpool"
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

	workerPool *workerpool.WorkerPool

	wg   *sync.WaitGroup
}

func NewKafkaProducer(
	n uint64,
	sender sender.EventSender,
	events <-chan model.CorrectionEvent,
	workerPool *workerpool.WorkerPool,
	updater updater.Updater,
	cleaner cleaner.Cleaner,
) Producer {

	wg := &sync.WaitGroup{}

	return &producer{
		n:          n,
		sender:     sender,
		events:     events,
		workerPool: workerPool,
		wg:         wg,
		updater: updater,
		cleaner: cleaner,
	}
}

func (p *producer) Start(ctx context.Context) {
	for i := uint64(0); i < p.n; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for {
				select {
				case event := <-p.events:
					if err := p.sender.Send(&event); err != nil {
						log.Printf("Error while sending event with ID = %d to Kafka: %v\n", event.ID, err)
						p.workerPool.Submit(func() {
							if err := p.updater.Update(event); err != nil {
								log.Printf("Error while updating event with ID = %d: %v\n", event.ID, err)
							}
						})
					} else {
						log.Printf("Event with ID = %d was successfully sent to Kafka\n", event.ID)
						p.workerPool.Submit(func() {
							if err := p.cleaner.Clean(event); err != nil {
								log.Printf("Error while cleaning event with ID = %d: %v\n", event.ID, err)
							}
						})
					}
				case <- ctx.Done():
					return
				}
			}
		}()
	}
}

func (p *producer) Close() {
	p.wg.Wait()
}
