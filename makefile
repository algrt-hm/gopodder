
all: gopodder

dev: gopodder
#	go test -timeout 30s -run ^TestLastestPodsFromDb$$ gopodder
	GOPODCONF="/titanium/new_podcasts" GOPODDIR="/titanium/new_podcasts" ./gopodder -l

gopodder:
	go build gopodder.go

clean:
	rm ./gopodder

.PHONY: clean