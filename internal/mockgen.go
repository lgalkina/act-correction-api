package internal

//go:generate mockgen -destination=./mocks/repo_mock.go -package=mocks github.com/lgalkina/act-correction-api/internal/app/repo EventRepo
//go:generate mockgen -destination=./mocks/sender_mock.go -package=mocks github.com/lgalkina/act-correction-api/internal/app/sender EventSender
