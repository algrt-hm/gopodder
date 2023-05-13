
all: gopodder

gopodder:
	go build gopodder.go

clean:
	rm ./gopodder

.PHONY: clean