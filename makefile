.POSIX:

name = gopodder

# Pick the Go binary in the recipe shell so this works under both GNU make
# (macOS) and BSD make (FreeBSD): GNU's $(shell ...) and BSD's != operator are
# mutually unsupported, but both hand recipes to /bin/sh identically.
# Defaults to go125 on FreeBSD (pkg installs a versioned binary), else go.
# Override by setting GO in the environment or on the command line, e.g.
# `make GO=go1.25` or `GO=go1.25 make`.
GO_CMD = $${GO:-$$([ "$$(uname -s)" = FreeBSD ] && echo go125 || echo go)}

all: build

format:
	$(GO_CMD) fmt *.go

test:
	$(GO_CMD) test ./...

build:
	$(GO_CMD) build -o $(name) *.go

interactive: build
	./$(name) --interactive

DEV_GOPODDIR ?= /mnt/titanium/new_podcasts

dev: build
	GOPODDIR=$(DEV_GOPODDIR) ./$(name) --interactive

clean:
	rm ./$(name)

.PHONY: clean
