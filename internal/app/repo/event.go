package repo

import (
	"github.com/lgalkina/act-correction-api/internal/model"
)

type EventRepo interface {
	Lock(n uint64) ([]model.CorrectionEvent, error)
	Unlock(eventIDs []uint64) error

	Add(event []model.CorrectionEvent) error
	Remove(eventIDs []uint64) error
}