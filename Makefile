.PHONY: build
build:
	go build cmd/act-correction-api/main.go

.PHONY: test
test:
	go test -v ./...