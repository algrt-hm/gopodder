.POSIX:

name = gopodder

all: build

format:
	go fmt *.go

test:
	go test ./...

build:
	go build -o $(name) *.go

interactive: build
	./$(name) --interactive

DEV_GOPODDIR ?= /mnt/titanium/new_podcasts

dev: build
	GOPODDIR=$(DEV_GOPODDIR) ./$(name) --interactive

clean:
	rm ./$(name)

.PHONY: clean
