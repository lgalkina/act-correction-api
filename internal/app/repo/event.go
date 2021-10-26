package repo

import (
	"github.com/lgalkina/act-correction-api/internal/model/activity"
)

type EventRepo interface {
	Lock(n uint64) ([]activity.CorrectionEvent, error)
	Unlock(eventIDs []uint64) error

	Add(event []activity.CorrectionEvent) error
	Remove(eventIDs []uint64) error
}
