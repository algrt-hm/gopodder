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
pkg install go
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

- Interactive mode first tries to load podcast titles from `gopodder.sqlite` (`interactive_episodes` table), and you pick by podcast title
- If the DB has no interactive podcast rows yet, it falls back to `$GOPODDIR/gopodder-extra.conf` (one URL per line), and then to manual URL entry
- Press `m` to enter a URL manually
- When a feed URL is selected in interactive mode, its parsed podcast/episode metadata is written into `interactive_episodes`, so next runs can show that podcast by title
- The UI lists episodes (most recent first). It starts with the latest 10 and you can press `a` to expand to the full list
- Episodes already in the `downloads` table are marked with a `✓`. Press `d` to toggle hiding downloaded episodes
- Select episodes with Space, then choose a destination folder. Downloads happen immediately and mp3 tags are written (uses `wget` and `eyeD3`)
- Successful interactive downloads are also recorded in the `downloads` table

Note: the `interactive_episodes` table is populated during feed parsing (`-p` / `-a`); existing `episodes` rows are not backfilled automatically.

### Archiving older podcasts

If your podcast directory has grown large, you can off-load older files to a different volume without having `gopodder` re-download them on the next run. There are two complementary mechanisms:

1. **`$GOPODDIR_ARCHIVES`** — a `:`-separated list of additional directories to scan for already-downloaded files. Use this when the archive volume is normally mounted.

    ``` shell
    export GOPODDIR_ARCHIVES=/mnt/bronze/new_podcasts_archive
    ./gopodder -s
    ```

    If a configured archive path can't be read at scan time, `gopodder` aborts loudly rather than silently treating files as missing.

2. **Archive registry** (`archived_episodes` table) — a DB-backed list of episode hashes that should be treated as already-downloaded regardless of whether the volume is currently mounted.

    ``` shell
    # After you've moved files to the archive volume:
    ./gopodder --register-archive /mnt/bronze/new_podcasts_archive

    # If you move files back into the primary dir:
    ./gopodder --unregister-archive /mnt/bronze/new_podcasts_archive

    # Drop registry rows whose archived path no longer exists on disk:
    ./gopodder --reconcile-archive
    ```

    Each of these is a one-shot command (run-and-exit); they don't combine with `-p/-s/-d/-u/-t`.

The two mechanisms compose: registered hashes are always treated as "have"; mounted archive paths additionally provide visibility for tools that want to inspect filenames. For an off-line/cold archive, the registry alone is sufficient.

The interactive picker also marks episodes as already-downloaded if they are in either `downloads` or `archived_episodes`.

### Retitled episodes and deduplication

Some feeds republish the same episode under a new title, repeatedly in the worst cases. Because episodes are keyed on an MD5 of `podcast_title` + `episode_title`, a retitle looks like a brand-new episode and would be downloaded again.

Two defences exist:

1. **Download-time guard** (automatic, part of `-s`/`-a`): before an episode is added to the download script, it is skipped if another row with the same feed `guid` already has a file — the guid is the feed's own episode identity and is stable across retitles. A fallback catches guid-rotating feeds: same podcast, same published date, and materially overlapping titles (guarded so that "Part 1"/"Part 2" siblings and same-day episodes of daily feeds are never merged). Every skip is logged and recorded in the `skipped_episodes` table with the reason and the matched episode, so refusals are auditable:

    ``` sql
    select * from skipped_episodes order by last_skipped desc;
    ```

2. **Cleanup passes** (one-shot commands, dry-run by default) for duplicates that are already on disk:

    ``` shell
    ./gopodder --dedup-guid       # plan merging copies of retitled episodes (same guid, different hash)
    ./gopodder --dedup-guid-delete    # apply it

    ./gopodder --dedup-twins      # same, for copies under the same filename prefix (rotated URL hashes)
    ./gopodder --dedup-retitles   # same, for duplicates split across a podcast *rename*
    ./gopodder --prune-stale-episodes # drop stale episode rows that would re-queue deleted variants
    ```

    Each pass prints its plan (`delete`/`rename`/`MANUAL`) and only touches files when a surviving copy of the same episode is kept; anything the evidence doesn't decide is reported `MANUAL` and left alone. The `-delete` variants also maintain `downloads`, `archived_episodes`, and stale `episodes` rows in the same transaction.

### To install dependencies

- MacOS: `brew install eye-d3 wget`
- Linux (Debian-based): `sudo apt install eyed3 wget`

## Technical

### Data model

Database Design (SQLite)

Six tables: `podcasts`, `episodes`, `interactive_episodes`, `downloads`, `archived_episodes`, and `skipped_episodes`.

- `podcasts` uses `title` as the primary key. A feed renaming the whole show is detected at parse time (a majority of the feed's episode guids already belonging to one existing podcast) and applied as an in-place rename of the `podcasts` row and `episodes.podcast_title` — not a new record
- `episodes` and `interactive_episodes` are keyed on an MD5 hash of `podcast_title` + `episode_title`
    - The `interactive_episodes` table duplicates the `episodes` schema — this redundancy exists to separate batch vs. TUI concerns
    - A feed retitling an episode is matched back to its existing row at parse time by `guid` (corroborated by published date or title overlap) or by exact title; only an uncorroborated retitle creates a new row, which the download-time guard then refuses (see "Retitled episodes and deduplication" above)
    - **The hash is a stable identity, not a derivation.** It is what ties a row to its file on disk and to the archive registry, so it is never recomputed. After a podcast rename the back catalogue keeps hashes computed from the *old* podcast title, and the files keep their old-name filenames — e.g. since the 2026-07-09 "Arts & Ideas" → "Free Thinking" rename, that show's pre-rename rows carry `md5('Arts & Ideas' + episode_title)` and live on disk as `Arts_Ideas-*.mp3`. Ad-hoc queries, scripts, or new code must never assume `podcastname_episodename_hash == md5(podcast_title + title)` for existing rows; treat the stored hash as opaque
- `downloads` tracks filenames and tagging status (`tagged_at`)
- `archived_episodes` records episode hashes that have been off-loaded to another volume; rows here suppress re-download (see "Archiving older podcasts" above)
- `skipped_episodes` is the audit trail of downloads refused as retitle duplicates: the skipped episode, the matched sibling, the reason, and first/last skip timestamps
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
│ archive.go     │ Archive registry (register/unregister/reconcile)│
├────────────────┼─────────────────────────────────────────────────┤
│ dedup.go       │ Cleanup passes: twin/retitle/guid duplicate     │
│                │ merging and stale-row pruning                   │
├────────────────┼─────────────────────────────────────────────────┤
│ skip.go        │ Download-time retitle detection (guid + title   │
│                │ heuristics)                                     │
├────────────────┼─────────────────────────────────────────────────┤
│ interactive.go │ Bubble Tea TUI (multi-step episode picker)      │
├────────────────┼─────────────────────────────────────────────────┤
│ httprss.go     │ RSS feed fetching/parsing via gofeed            │
├────────────────┼─────────────────────────────────────────────────┤
│ utils.go       │ Text cleaning, path handling, dependency checks │
└────────────────┴─────────────────────────────────────────────────┘
```

The batch workflow runs as a 5-stage pipeline: parse feeds → generate download list → download → update DB → tag MP3s. The generate stage applies two skip checks before queueing anything: the prefix twin backstop (same canonical filename under another hash) and the retitle guard from `skip.go` (see "Retitled episodes and deduplication").

The interactive mode is a separate state-machine driven by Bubble Tea with 7 steps (URL entry → feed select → loading → episode
select → folder → downloading → done).