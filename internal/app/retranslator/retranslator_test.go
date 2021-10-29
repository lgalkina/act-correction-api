package retranslator

import (
	"context"
	"errors"
	"github.com/lgalkina/act-correction-api/internal/model"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/lgalkina/act-correction-api/internal/mocks"
	"github.com/stretchr/testify/assert"
)

func TestStart(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockEventRepo(ctrl)
	repo.EXPECT().Lock(gomock.Any()).AnyTimes()
	sender := mocks.NewMockEventSender(ctrl)

	retranslator, err := NewRetranslator(createRetranslatorConfig(2, time.Second, repo, sender))
	assert.Nil(t, err)

	retranslator.Start(context.Background())
	time.Sleep(time.Second * 2)
	retranslator.Close()
}

func TestProducerSendSuccess(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockEventRepo(ctrl)
	sender := mocks.NewMockEventSender(ctrl)

	events := createEvents()
	var consumeSize = uint64(len(events) / 2)

	retranslator, err := NewRetranslator(createRetranslatorConfig(consumeSize, time.Second, repo, sender))
	assert.Nil(t, err)

	// lock возвращает данные дважды
	setEventsLockOrder(consumeSize, repo, events)
	setEventsSend(int(consumeSize*2), sender, events,nil)
	eventIDs := setEventsRemove(repo, events, nil)

	ctx := context.Background()
	retranslator.Start(ctx)
	time.Sleep(time.Second * 2)
	retranslator.Close()

	assert.Equal(t, len(eventIDs), 0)
}

func TestProducerSendRemoveError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockEventRepo(ctrl)
	sender := mocks.NewMockEventSender(ctrl)

	events := createEvents()
	var consumeSize = uint64(len(events) / 2)

	retranslator, err := NewRetranslator(createRetranslatorConfig(consumeSize, time.Second, repo, sender))
	assert.Nil(t, err)

	setEventsLockOrder(consumeSize, repo, events)
	setEventsSend(int(consumeSize*2), sender, events,nil)
	eventIDs := setEventsRemove(repo, events, errors.New("remove error"))

	ctx := context.Background()
	retranslator.Start(ctx)
	time.Sleep(time.Second * 2)
	retranslator.Close()

	assert.Equal(t, len(eventIDs), 0)
}

func TestProducerSendError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockEventRepo(ctrl)
	sender := mocks.NewMockEventSender(ctrl)

	events := createEvents()
	var consumeSize = uint64(len(events) / 2)

	retranslator, err := NewRetranslator(createRetranslatorConfig(consumeSize, time.Second, repo, sender))
	assert.Nil(t, err)

	setEventsLockOrder(consumeSize, repo, events)
	setEventsSend(int(consumeSize*2), sender, events, errors.New("send error"))

	eventIDs := setEventsUnlock(repo, events, nil)

	ctx := context.Background()
	retranslator.Start(ctx)
	time.Sleep(time.Second * 2)
	retranslator.Close()

	assert.Equal(t, len(eventIDs), 0)
}

func TestProducerSendUnlockError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockEventRepo(ctrl)
	sender := mocks.NewMockEventSender(ctrl)

	events := createEvents()
	var consumeSize = uint64(len(events) / 2)

	retranslator, err := NewRetranslator(createRetranslatorConfig(consumeSize, time.Second, repo, sender))
	assert.Nil(t, err)

	setEventsLockOrder(consumeSize, repo, events)
	setEventsSend(int(consumeSize*2), sender, events, errors.New("send error"))
	eventIDs := setEventsUnlock(repo, events, errors.New("unlock error"))

	ctx := context.Background()
	retranslator.Start(ctx)
	time.Sleep(time.Second * 2)
	retranslator.Close()

	assert.Equal(t, len(eventIDs), 0)
}

func setEventsLockOrder(consumeSize uint64, repo *mocks.MockEventRepo, events []model.CorrectionEvent) {
	gomock.InOrder(
		repo.EXPECT().Lock(gomock.Eq(consumeSize)).Return(events[:consumeSize], nil).Times(1),
		repo.EXPECT().Lock(gomock.Eq(consumeSize)).Return(events[consumeSize:consumeSize*2], nil).Times(1),
		repo.EXPECT().Lock(gomock.Any()).AnyTimes(),
	)
}

func setEventsSend(eventsNum int, sender *mocks.MockEventSender, events []model.CorrectionEvent, error interface{}) {
	for i := 0; i < eventsNum; i++ {
		sender.EXPECT().Send(&events[i]).Return(error).Times(1)
	}
}

func setEventsRemove(repo *mocks.MockEventRepo, events []model.CorrectionEvent, error interface{}) map[uint64]bool {
	eventIDs := make(map[uint64]bool, len(events))
	for _, event := range events {
		eventIDs[event.ID] = true
	}
	repo.EXPECT().Remove(gomock.Any()).Do(func(ids []uint64) {
		for _, id := range ids {
			delete(eventIDs, id)
		}
	}).Return(error).AnyTimes()
	return eventIDs
}

func setEventsUnlock(repo *mocks.MockEventRepo, events []model.CorrectionEvent, error interface{}) map[uint64]bool {
	eventIDs := make(map[uint64]bool, len(events))
	for _, event := range events {
		eventIDs[event.ID] = true
	}
	repo.EXPECT().Unlock(gomock.Any()).Do(func(ids []uint64) {
		for _, id := range ids {
			delete(eventIDs, id)
		}
	}).Return(error).AnyTimes()
	return eventIDs
}

func createRetranslatorConfig(consumeSize uint64, consumeTimeout time.Duration,
								repo *mocks.MockEventRepo, sender *mocks.MockEventSender) Config {
	return Config{
		ChannelSize:    512,
		ConsumerCount:  2,
		ConsumeSize:    consumeSize,
		ConsumeTimeout: consumeTimeout,
		ProducerCount:  2,
		WorkerCount:    2,
		Repo:           repo,
		Sender:         sender,
	}
}

func createEvents() []model.CorrectionEvent {
	return []model.CorrectionEvent{
		{
			ID:     1,
			Type:   model.Created,
			Status: model.Deferred,
			Entity: &model.Correction{ID: 1, Timestamp: time.Now(), UserID: 1, Object: "order1 description", Action: "update",
				Data: &model.Data{OriginalData: "test11", RevisedData: "test12"}},
		},
		{
			ID:     2,
			Type:   model.Created,
			Status: model.Deferred,
			Entity: &model.Correction{ID: 2, Timestamp: time.Now(), UserID: 2, Object: "order2 description", Action: "update",
				Data: &model.Data{OriginalData: "test21", RevisedData: "test22"}},
		},
		{
			ID:     3,
			Type:   model.Created,
			Status: model.Deferred,
			Entity: &model.Correction{ID: 3, Timestamp: time.Now(), UserID: 3, Object: "order3 description", Action: "update",
				Data: &model.Data{OriginalData: "test31", RevisedData: "test32"}},
		},
		{
			ID:     4,
			Type:   model.Created,
			Status: model.Deferred,
			Entity: &model.Correction{ID: 4, Timestamp: time.Now(), UserID: 4, Object: "order4 description", Action: "update",
				Data: &model.Data{OriginalData: "test41", RevisedData: "test42"}},
		},
		{
			ID:     5,
			Type:   model.Created,
			Status: model.Deferred,
			Entity: &model.Correction{ID: 5, Timestamp: time.Now(), UserID: 5, Object: "order5 description", Action: "update",
				Data: &model.Data{OriginalData: "test51", RevisedData: "test52"}},
		},
		{
			ID:     6,
			Type:   model.Created,
			Status: model.Deferred,
			Entity: &model.Correction{ID: 6, Timestamp: time.Now(), UserID: 6, Object: "order6 description", Action: "update",
				Data: &model.Data{OriginalData: "test61", RevisedData: "test62"}},
		},
	}
}