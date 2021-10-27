package updater

import (
	"github.com/lgalkina/act-correction-api/internal/app/repo"
)

type Updater interface {
	Update(eventID uint64) error
}

type updater struct {
	repo repo.EventRepo
}

func NewDbUpdater(repo repo.EventRepo) Updater {
	return &updater {
		repo: repo,
	}
}

func (u *updater) Update(eventID uint64) error {
	return u.repo.Unlock([]uint64{eventID})
}
