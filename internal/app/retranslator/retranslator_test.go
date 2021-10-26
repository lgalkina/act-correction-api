package retranslator

import (
	"errors"
	"github.com/lgalkina/act-correction-api/internal/model"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/lgalkina/act-correction-api/internal/mocks"
)

func TestStart(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockEventRepo(ctrl)
	repo.EXPECT().Lock(gomock.Any()).AnyTimes()
	sender := mocks.NewMockEventSender(ctrl)

	retranslator := NewRetranslator(createRetranslatorConfig(2, time.Second, repo, sender))
	retranslator.Start()
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
	retranslator := NewRetranslator(createRetranslatorConfig(consumeSize, time.Second, repo, sender))

	// lock возвращает данные дважды
	setEventsLockOrder(consumeSize, repo, events)
	setEventsSend(int(consumeSize*2), sender, events,nil)
	setEventsRemove(int(consumeSize*2), repo, events, nil)

	retranslator.Start()
	time.Sleep(time.Second * 2)
	retranslator.Close()
}

func TestProducerSendRemoveError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockEventRepo(ctrl)
	sender := mocks.NewMockEventSender(ctrl)

	events := createEvents()
	var consumeSize = uint64(len(events) / 2)
	retranslator := NewRetranslator(createRetranslatorConfig(consumeSize, time.Second, repo, sender))

	setEventsLockOrder(consumeSize, repo, events)
	setEventsSend(int(consumeSize*2), sender, events,nil)
	setEventsRemove(int(consumeSize*2), repo, events, errors.New("remove error"))

	retranslator.Start()
	time.Sleep(time.Second * 2)
	retranslator.Close()
}

func TestProducerSendError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockEventRepo(ctrl)
	sender := mocks.NewMockEventSender(ctrl)

	events := createEvents()
	var consumeSize = uint64(len(events) / 2)
	retranslator := NewRetranslator(createRetranslatorConfig(consumeSize, time.Second, repo, sender))

	setEventsLockOrder(consumeSize, repo, events)
	setEventsSend(int(consumeSize*2), sender, events, errors.New("send error"))
	setEventsUnlock(int(consumeSize*2), repo, events, nil)

	retranslator.Start()
	time.Sleep(time.Second * 2)
	retranslator.Close()
}

func TestProducerSendUnlockError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockEventRepo(ctrl)
	sender := mocks.NewMockEventSender(ctrl)

	events := createEvents()
	var consumeSize = uint64(len(events) / 2)
	retranslator := NewRetranslator(createRetranslatorConfig(consumeSize, time.Second, repo, sender))

	setEventsLockOrder(consumeSize, repo, events)
	setEventsSend(int(consumeSize*2), sender, events, errors.New("send error"))
	setEventsUnlock(int(consumeSize*2), repo, events, errors.New("unlock error"))

	retranslator.Start()
	time.Sleep(time.Second * 2)
	retranslator.Close()
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

func setEventsRemove(eventsNum int, repo *mocks.MockEventRepo, events []model.CorrectionEvent, error interface{}) {
	for i := 0; i < eventsNum; i++ {
		repo.EXPECT().Remove([]uint64{events[i].ID}).Return(error).Times(1)
	}
}

func setEventsUnlock(eventsNum int, repo *mocks.MockEventRepo, events []model.CorrectionEvent, error interface{}) {
	for i := 0; i < eventsNum; i++ {
		repo.EXPECT().Unlock([]uint64{events[i].ID}).Return(error).Times(1)
	}
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
			Status: model.Processed,
			Entity: &model.Correction{ID: 1, Timestamp: time.Now(), UserID: 1, Object: "order1 description", Action: "update",
				Data: &model.Data{OriginalData: "test11", RevisedData: "test12"}},
		},
		{
			ID:     2,
			Type:   model.Created,
			Status: model.Processed,
			Entity: &model.Correction{ID: 2, Timestamp: time.Now(), UserID: 2, Object: "order2 description", Action: "update",
				Data: &model.Data{OriginalData: "test21", RevisedData: "test22"}},
		},
		{
			ID:     3,
			Type:   model.Created,
			Status: model.Processed,
			Entity: &model.Correction{ID: 3, Timestamp: time.Now(), UserID: 3, Object: "order3 description", Action: "update",
				Data: &model.Data{OriginalData: "test31", RevisedData: "test32"}},
		},
		{
			ID:     4,
			Type:   model.Created,
			Status: model.Processed,
			Entity: &model.Correction{ID: 4, Timestamp: time.Now(), UserID: 4, Object: "order4 description", Action: "update",
				Data: &model.Data{OriginalData: "test41", RevisedData: "test42"}},
		},
		{
			ID:     5,
			Type:   model.Created,
			Status: model.Processed,
			Entity: &model.Correction{ID: 5, Timestamp: time.Now(), UserID: 5, Object: "order5 description", Action: "update",
				Data: &model.Data{OriginalData: "test51", RevisedData: "test52"}},
		},
		{
			ID:     6,
			Type:   model.Created,
			Status: model.Processed,
			Entity: &model.Correction{ID: 5, Timestamp: time.Now(), UserID: 6, Object: "order6 description", Action: "update",
				Data: &model.Data{OriginalData: "test61", RevisedData: "test62"}},
		},
	}
}