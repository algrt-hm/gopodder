
all: gopodder

run:
	export GOPODCONF=/mnt/titanium/new_podcasts; \
	export GOPODDIR=/mnt/titanium/new_podcasts; \
	./gopodder -h

dev-tag-mnt:
	cd /mnt/titanium/new_podcasts && \
	export GOPODCONF=/mnt/titanium/new_podcasts; \
	export GOPODDIR=/mnt/titanium/new_podcasts; \
	dlv debug github.com/algrt-hm/gopodder -- -t 

dev-tag-local:
	export GOPODCONF=/titanium/new_podcasts; \
	export GOPODDIR=/titanium/new_podcasts; \
	dlv debug github.com/algrt-hm/gopodder -- -t

gopodder:
	go build gopodder.go

clean:
	rm ./gopodder

.PHONY: clean
