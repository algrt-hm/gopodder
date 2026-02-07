.POSIX:

name = gopodder

all: build

format:
	go fmt *.go

dev:
	go test -timeout 30s -run ^TestParseLogic

test:
	go test ./...

build:
	go build -o $(name) *.go

interactive: build
	./$(name) --interactive

clean:
	rm ./$(name)

.PHONY: clean
