package producer

import (
	"context"
	"errors"
	"github.com/gammazero/workerpool"
	"github.com/golang/mock/gomock"
	"github.com/lgalkina/act-correction-api/internal/app/cleaner"
	"github.com/lgalkina/act-correction-api/internal/app/updater"
	"github.com/lgalkina/act-correction-api/internal/mocks"
	"github.com/lgalkina/act-correction-api/internal/model"
	"testing"
	"time"
)

var testEvent = model.CorrectionEvent{
	ID:     1,
	Type:   model.Created,
	Status: model.Deferred,
	Entity: &model.Correction{ID: 1, Timestamp: time.Now(), UserID: 1, Object: "order1 description", Action: "update",
		Data: &model.Data{OriginalData: "test11", RevisedData: "test12"}},
}

func TestRemove(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockEventRepo(ctrl)
	sender := mocks.NewMockEventSender(ctrl)

	sender.EXPECT().Send(&testEvent).Return(nil).Times(1)
	repo.EXPECT().Remove(gomock.Eq([]uint64{testEvent.ID})).Return(nil).Times(1)

	events := make(chan model.CorrectionEvent)
	workerpool := workerpool.New(1)

	producer := NewKafkaProducer(
		1,
		sender,
		events,
		updater.NewDbUpdater(repo, workerpool, 0),
		cleaner.NewDbCleaner(repo, workerpool, 0))

	cancelCtx, cancelCtxFunc := createCtx()

	producer.Start(cancelCtx)
	events <- testEvent
	time.Sleep(time.Second)

	cancelCtxFunc()
	producer.Close()
}

func TestUnlock(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockEventRepo(ctrl)
	sender := mocks.NewMockEventSender(ctrl)

	sender.EXPECT().Send(&testEvent).Return(errors.New("send error")).Times(1)
	repo.EXPECT().Unlock(gomock.Eq([]uint64{testEvent.ID})).Return(nil).Times(1)

	events := make(chan model.CorrectionEvent)
	workerpool := workerpool.New(1)

	producer := NewKafkaProducer(
		1,
		sender,
		events,
		updater.NewDbUpdater(repo, workerpool, 0),
		cleaner.NewDbCleaner(repo, workerpool, 0))

	cancelCtx, cancelCtxFunc := createCtx()

	producer.Start(cancelCtx)
	events <- testEvent
	time.Sleep(time.Second)

	cancelCtxFunc()
	producer.Close()
}

func createCtx() (context.Context, context.CancelFunc) {
	ctx := context.Background()
	return context.WithCancel(ctx)
}