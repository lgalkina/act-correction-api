package cleaner

import (
	"github.com/lgalkina/act-correction-api/internal/app/repo"
	"github.com/lgalkina/act-correction-api/internal/model"
)

type Cleaner interface {
	Clean(event model.CorrectionEvent) error
}

type cleaner struct {
	repo repo.EventRepo
}

func NewDbCleaner(repo repo.EventRepo) Cleaner {
	return &cleaner {
		repo: repo,
	}
}

func (c *cleaner) Clean(event model.CorrectionEvent) error {
	return c.repo.Remove([]uint64{event.ID})
}
