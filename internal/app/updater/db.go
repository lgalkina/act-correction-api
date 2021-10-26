package updater

import (
	"github.com/lgalkina/act-correction-api/internal/app/repo"
	"github.com/lgalkina/act-correction-api/internal/model"
)

type Updater interface {
	Update(event model.CorrectionEvent) error
}

type updater struct {
	repo repo.EventRepo
}

func NewDbUpdater(repo repo.EventRepo) Updater {
	return &updater {
		repo: repo,
	}
}

func (u *updater) Update(event model.CorrectionEvent) error {
	return u.repo.Unlock([]uint64{event.ID})
}
