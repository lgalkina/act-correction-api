package cleaner

import (
	"context"
	"github.com/gammazero/workerpool"
	"github.com/lgalkina/act-correction-api/internal/app/repo"
	"log"
)

const cleanBatchSize = 5

type Cleaner interface {
	Start(ctx context.Context)
	Close()

	Add(eventID uint64)
	Clean(eventIDs []uint64) error
}

type cleaner struct {
	repo 	   repo.EventRepo
	eventIDs   chan uint64
	workerPool *workerpool.WorkerPool
}

func NewDbCleaner(repo repo.EventRepo,
	workerPool *workerpool.WorkerPool,
	channelSize uint64) Cleaner {
	eventIDs := make(chan uint64, channelSize)
	return &cleaner {
		repo: repo,
		eventIDs: eventIDs,
		workerPool: workerPool,
	}
}

func (c *cleaner) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case eventID, ok := <-c.eventIDs:
				if ok {
					c.processEventID(eventID)
				} else {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (c *cleaner) Close() {
	close(c.eventIDs)
}

func (c *cleaner) Add(eventID uint64) {
	c.eventIDs <- eventID
}

func (c *cleaner) Clean(eventIDs []uint64) error {
	return c.repo.Remove(eventIDs)
}

func (c *cleaner) processEventID(eventID uint64) {
	eventIDs := []uint64{eventID}
	forLabel:
		for {
			select {
			case id := <-c.eventIDs:
				// if there is more id immediately available, we add them to our slice
				eventIDs = append(eventIDs, id)
				if len(eventIDs) >= cleanBatchSize {
					break forLabel
				}
			default:
				// else we move on without blocking
				break forLabel
			}
		}
		c.workerPool.Submit(func() {
			if err := c.Clean(eventIDs); err != nil {
				log.Printf("Error while cleaning events: %v\n", err)
			}
		})
}