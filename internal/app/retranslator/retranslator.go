package retranslator

import (
	"errors"
	"github.com/lgalkina/act-correction-api/internal/app/cleaner"
	"github.com/lgalkina/act-correction-api/internal/app/updater"
	"github.com/lgalkina/act-correction-api/internal/model"
	"time"

	"github.com/gammazero/workerpool"
	"github.com/lgalkina/act-correction-api/internal/app/consumer"
	"github.com/lgalkina/act-correction-api/internal/app/producer"
	"github.com/lgalkina/act-correction-api/internal/app/repo"
	"github.com/lgalkina/act-correction-api/internal/app/sender"
)

type Retranslator interface {
	Start()
	Close()
}

type Config struct {
	ChannelSize uint64

	ConsumerCount  uint64
	ConsumeSize    uint64
	ConsumeTimeout time.Duration

	ProducerCount uint64
	WorkerCount   int

	Repo   repo.EventRepo
	Sender sender.EventSender
}

type retranslator struct {
	events     chan model.CorrectionEvent
	consumer   consumer.Consumer
	producer   producer.Producer
	workerPool *workerpool.WorkerPool
}

func NewRetranslator(cfg Config) (Retranslator, error) {

	err := validateConfig(cfg)
	if err != nil {
		return nil, err
	}

	events := make(chan model.CorrectionEvent, cfg.ChannelSize)
	workerPool := workerpool.New(cfg.WorkerCount)

	updater := updater.NewDbUpdater(cfg.Repo)
	cleaner := cleaner.NewDbCleaner(cfg.Repo)

	consumer := consumer.NewDbConsumer(
		cfg.ConsumerCount,
		cfg.ConsumeSize,
		cfg.ConsumeTimeout,
		cfg.Repo,
		events)
	producer := producer.NewKafkaProducer(
		cfg.ProducerCount,
		cfg.Sender,
		events,
		workerPool,
		updater,
		cleaner)

	return &retranslator{
		events:     events,
		consumer:   consumer,
		producer:   producer,
		workerPool: workerPool,
	}, nil
}

func (r *retranslator) Start() {
	r.producer.Start()
	r.consumer.Start()
}

func (r *retranslator) Close() {
	r.consumer.Close()
	r.producer.Close()
	r.workerPool.StopWait()
}

func validateConfig(cfg Config) error {
	switch {
		case cfg.ConsumerCount < 1:
			return errors.New("ConsumerCount can't be less than 1")
		case cfg.ProducerCount < 1:
			return errors.New("ProducerCount can't be less than 1")
		case cfg.WorkerCount < 1:
			return errors.New("WorkerCount can't be less than 1")
		case cfg.ConsumeSize < 1:
			return errors.New("ConsumeSize can't be less than 1")
		case cfg.ConsumeTimeout < 1:
			return errors.New("ConsumeTimeout can't be less than 1")
	default:
		return nil
	}
}