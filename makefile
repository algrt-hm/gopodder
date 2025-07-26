.POSIX:

name = gopodder

all: build

format:
	go fmt *.go

dev:
	go test -timeout 30s -run ^TestParseLogic

# TODO
test: clean $(name)
#	go test -timeout 30s -run ^TestLastestPodsFromDb$$ gopodder
#	go test -timeout 30s -run ^TestCheckDependencies$$ gopodder
	echo 'nothing'

build:
	go build -o $(name) *.go

clean:
	rm ./$(name)

.PHONY: clean
