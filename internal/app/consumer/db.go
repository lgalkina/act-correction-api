package consumer

import (
	"context"
	"github.com/lgalkina/act-correction-api/internal/model"
	"sync"
	"time"

	"github.com/lgalkina/act-correction-api/internal/app/repo"
)

type Consumer interface {
	Start(ctx context.Context)
	Close()
}

type consumer struct {
	n      uint64
	events chan<- model.CorrectionEvent

	repo repo.EventRepo

	batchSize uint64
	timeout   time.Duration

	wg   *sync.WaitGroup
}

type Config struct {
	n         uint64
	events    chan<- model.CorrectionEvent
	repo      repo.EventRepo
	batchSize uint64
	timeout   time.Duration
}

func NewDbConsumer(
	n uint64,
	batchSize uint64,
	consumeTimeout time.Duration,
	repo repo.EventRepo,
	events chan<- model.CorrectionEvent) Consumer {

	wg := &sync.WaitGroup{}

	return &consumer{
		n:         n,
		batchSize: batchSize,
		timeout:   consumeTimeout,
		repo:      repo,
		events:    events,
		wg:        wg,
	}
}

func (c *consumer) Start(ctx context.Context) {
	for i := uint64(0); i < c.n; i++ {
		c.wg.Add(1)
		go c.processEvents(ctx)
	}
}

func (c *consumer) Close() {
	c.wg.Wait()
}

func (c *consumer) processEvents(ctx context.Context) {
	defer c.wg.Done()
	ticker := time.NewTicker(c.timeout)
	for {
		select {
		case <-ticker.C:
			events, err := c.repo.Lock(c.batchSize)
			if err != nil {
				continue
			}
			for _, event := range events {
				c.events <- event
			}
		case <- ctx.Done():
			return
		}
	}
}