# gopodder

## What

`gopodder` is a podcast downloader which will tag mp3s with correct metadata from RSS feed.

## Where

Builds and runs nicely on MacOS, linux, FreeBSD.

## Why

Many podcasts have good data in the RSS feed on the episode and series names etc, but poor metadata in the mp3 file itself; `gopodder` writes to the mp3 the metadata from the RSS feed at the time of download.

If you like your podcasts as mp3s all in one folder with sensible file names and proper tags, `gopodder` may very well be for you.

## How to use

``` shell
./gopodder -a
```

Please see below for more detail

### Setup

It's very easy to download, compile and run if you have the go toolchain set up

Firstly, to install the go toolchain if you do not have it already:

``` shell
# On Ubuntu/Pop!_OS 22.04 'Jammy'
sudo apt install golang-1.18

# On MacOS
brew install go

# On FreeBSD
TBD
```

Secondly, `go build`

``` shell
git clone https://github.com/algrt-hm/gopodder.git
cd gopodder
go build
./gopodder -h
```

`gopodder` requires `wget` and `eyeD3` to both be installed (highly likely to be available via your package manager e.g. apt, brew, etc)

### How I use it

I use `gopodder` in a cron script like the below.

1. Firstly, a script that can be easily run from cron

``` shell
#!/bin/sh
cd /home/user/podcasts
GOPODCONF=`pwd` GOPODDIR=`pwd` ./gopodder -a >> pods.log 2>&1
```

2. Secondly, the line in the cron script itself to run every day at 7am.

``` cron
# Podcasts: “At 07:00”
0 7 * * *    /home/user/podcasts/cron.sh
```

3. Thirdly you will need a configuration file, `gopodder.conf`, which is a list of RSS feeds to read from, e.g.

``` none
https://lexfridman.com/feed/podcast/
```