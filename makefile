.POSIX:

name = gopodder

GO ?= $(shell [ "$$(uname -s)" = "FreeBSD" ] && echo go125 || echo go)

all: build

format:
	$(GO) fmt *.go

test:
	$(GO) test ./...

build:
	$(GO) build -o $(name) *.go

interactive: build
	./$(name) --interactive

DEV_GOPODDIR ?= /mnt/titanium/new_podcasts

dev: build
	GOPODDIR=$(DEV_GOPODDIR) ./$(name) --interactive

clean:
	rm ./$(name)

.PHONY: clean
