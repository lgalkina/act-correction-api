package updater

import (
	"context"
	"github.com/gammazero/workerpool"
	"github.com/lgalkina/act-correction-api/internal/app/repo"
	"log"
)

const updateBatchSize = 5

type Updater interface {
	Start(ctx context.Context)
	Close()

	Add(eventID uint64)
	Update(eventIDs []uint64) error
}

type updater struct {
	repo 	   repo.EventRepo
	eventIDs   chan uint64
	workerPool *workerpool.WorkerPool
}

func NewDbUpdater(repo repo.EventRepo,
	workerPool *workerpool.WorkerPool,
	channelSize uint64) Updater {
	eventIDs := make(chan uint64, channelSize)
	return &updater {
		repo: repo,
		eventIDs: eventIDs,
		workerPool: workerPool,
	}
}

func (u *updater) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case eventID, ok := <-u.eventIDs:
				if ok {
					u.processEventID(eventID)
				} else {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (u *updater) Close() {
	close(u.eventIDs)
}

func (u *updater) Add(eventID uint64) {
	u.eventIDs <- eventID
}

func (u *updater) Update(eventIDs []uint64) error {
	return u.repo.Unlock(eventIDs)
}

func (u *updater) processEventID(eventID uint64) {
	eventIDs := []uint64{eventID}
	forLabel:
		for {
			select {
			case id := <-u.eventIDs:
				// if there is more id immediately available, we add them
				eventIDs = append(eventIDs, id)
				if len(eventIDs) >= updateBatchSize {
					break forLabel
				}
			default:
				// else we move on without blocking
				break forLabel
			}
		}
		u.workerPool.Submit(func() {
			if err := u.Update(eventIDs); err != nil {
				log.Printf("Error while updating events: %v\n", err)
			}
		})
}