package cleaner

import (
	"github.com/lgalkina/act-correction-api/internal/app/repo"
)

type Cleaner interface {
	Clean(eventID uint64) error
}

type cleaner struct {
	repo repo.EventRepo
}

func NewDbCleaner(repo repo.EventRepo) Cleaner {
	return &cleaner {
		repo: repo,
	}
}

func (c *cleaner) Clean(eventID uint64) error {
	return c.repo.Remove([]uint64{eventID})
}
