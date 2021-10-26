package sender

import (
	"github.com/lgalkina/act-correction-api/internal/model/activity"
)

type EventSender interface {
	Send(subdomain *activity.CorrectionEvent) error
}
