# gopodder

### tl;dr

`gopodder` is a Go-based podcast downloader that fetches episodes from RSS feeds, downloads them with wget, and applies ID3v2 tags from feed metadata. It has two modes: a batch pipeline (cron-friendly) and an interactive TUI built with Bubble Tea.

## Overview

### What

`gopodder` is a podcast downloader which will tag mp3s with correct metadata from RSS feed.

### Where

Builds and runs nicely on MacOS, linux, FreeBSD.

### Why

Many podcasts have good data in the RSS feed on the episode and series names etc, but poor metadata in the mp3 file itself; `gopodder` writes to the mp3 the metadata from the RSS feed at the time of download.

If you like your podcasts as mp3s all in one folder with sensible file names and proper tags, `gopodder` may very well be for you.

### How to use 

Interactive:

``` shell
./gopodder -i
```

Batch mode:

``` shell
./gopodder -a
```

Please see below for more detail

### Setup

It's very easy to download, compile and run if you have the go toolchain set up

Firstly, to install the go toolchain if you do not have it already:

``` shell
# On Debian-based linux distribution e.g. Ubuntu
sudo apt install golang

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
(This assumes pods.log already exists, can `touch pods.log` if not)

2. Secondly, the line in the cron script itself to run every day at 7am.

``` cron
# Podcasts: “At 07:00”
0 7 * * *    /home/user/podcasts/cron.sh
```

3. Thirdly you will need a configuration file, `gopodder.conf`, which is a list of RSS feeds to read from, e.g.

``` none
https://lexfridman.com/feed/podcast/
```

### Interactive mode

Interactive mode allows you to pick the odd podcast from a podcast feed without downloading every episode.

Use interactive mode to pick and download individual episodes without editing `gopodder.conf`.

``` shell
./gopodder --interactive
```

Behaviour:

- Interactive mode first tries to load podcast titles from `gopodder.sqlite` (`interactive_episodes` table), and you pick by podcast title.
- If the DB has no interactive podcast rows yet, it falls back to `$GOPODDIR/gopodder-extra.conf` (one URL per line), and then to manual URL entry.
- Press `m` to enter a URL manually.
- When a feed URL is selected in interactive mode, its parsed podcast/episode metadata is written into `interactive_episodes`, so next runs can show that podcast by title.
- The UI lists episodes (most recent first). It starts with the latest 10 and you can press `a` to expand to the full list.
- Select episodes with Space, then choose a destination folder. Downloads happen immediately and mp3 tags are written (uses `wget` and `eyeD3`).
- Successful interactive downloads are also recorded in the `downloads` table.

Note: the `interactive_episodes` table is populated during feed parsing (`-p` / `-a`); existing `episodes` rows are not backfilled automatically.

### To install dependencies

- MacOS: `brew install eye-d3`
- Linux (Debian-based): `sudo apt install eyed3`

## Technical

### Data model

Database Design (SQLite)

Four tables: podcasts, episodes, interactive_episodes, and downloads.

- podcasts uses title as the primary key, meaning title changes would create a new record rather than updating.
- episodes and interactive_episodes are keyed on an MD5 hash of podcast_title + episode_title
    - The interactive_episodes table duplicates the episodes schema — this redundancy exists to separate batch vs. TUI concerns
- downloads tracks filenames and tagging status (tagged_at)
- No foreign key constraints exist between tables

### Dependencies

The project uses Go 1.24.2 with notable dependencies: gofeed (RSS parsing), go-sqlite3 (CGo SQLite driver), bubbletea/bubbles (TUI), id3v2 (tag writing), gomoji (emoji removal), and golang-set (set operations). The CGo dependency on go-sqlite3 means cross-compilation requires a C compiler.

### Code

The codebase is a flat, single-package Go project with clear file-level separation:

```
┌────────────────┬─────────────────────────────────────────────────┐
│      File      │                      Role                       │
├────────────────┼─────────────────────────────────────────────────┤
│ gopodder.go    │ Entry point, CLI args, batch orchestration      │
├────────────────┼─────────────────────────────────────────────────┤
│ db.go          │ SQLite schema, all database operations          │
├────────────────┼─────────────────────────────────────────────────┤
│ interactive.go │ Bubble Tea TUI (multi-step episode picker)      │
├────────────────┼─────────────────────────────────────────────────┤
│ httprss.go     │ RSS feed fetching/parsing via gofeed            │
├────────────────┼─────────────────────────────────────────────────┤
│ utils.go       │ Text cleaning, path handling, dependency checks │
└────────────────┴─────────────────────────────────────────────────┘
```

The batch workflow runs as a 5-stage pipeline: parse feeds → generate download list → download → update DB → tag MP3s. 

The interactive mode is a separate state-machine driven by Bubble Tea with 7 steps (URL entry → feed select → loading → episode
select → folder → downloading → done).