
all: gopodder

test:
	go test -timeout 30s -run ^TestLastestPodsFromDb$$ gopodder

gopodder:
	go build gopodder.go

clean:
	rm ./gopodder

.PHONY: clean