.POSIX:

all: gopodder

dev:
	GOPODCONF="/titanium/new_podcasts" GOPODDIR="/titanium/new_podcasts" ./gopodder -l

test: clean gopodder
#	go test -timeout 30s -run ^TestLastestPodsFromDb$$ gopodder
#	go test -timeout 30s -run ^TestCheckDependencies$$ gopodder
	GOPODCONF="/titanium/new_podcasts" GOPODDIR="/titanium/new_podcasts" ./gopodder -l

gopodder:
	go build gopodder.go

clean:
	rm ./gopodder

.PHONY: clean
