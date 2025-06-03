.POSIX:

name = gopodder

all: build

format:
	go fmt *.go

# TODO
dev:
	GOPODCONF="/titanium/new_podcasts" GOPODDIR="/titanium/new_podcasts" ./$(name) -l

# TODO
test: clean $(name)
#	go test -timeout 30s -run ^TestLastestPodsFromDb$$ gopodder
#	go test -timeout 30s -run ^TestCheckDependencies$$ gopodder
	GOPODCONF="/titanium/new_podcasts" GOPODDIR="/titanium/new_podcasts" ./$(name) -l

build:
	go build -o $(name) *.go

clean:
	rm ./$(name)

.PHONY: clean
