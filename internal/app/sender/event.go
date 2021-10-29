package sender

import (
	"github.com/lgalkina/act-correction-api/internal/model"
)

type EventSender interface {
	Send(subdomain *model.CorrectionEvent) error
}