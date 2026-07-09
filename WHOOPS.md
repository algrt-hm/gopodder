# Bugs found

## Whole podcast re-downloaded after publisher rename (2026-07-09)

The BBC renamed the show in feed `podcasts.files.bbci.co.uk/p02nrvk3.rss`
from "Arts & Ideas" to "Free Thinking" (programme link changed to
`b0144txn`). Podcasts are keyed by title and the episode hash is
MD5(podcast title + episode title), so the 09:55 run saw a brand-new
podcast, inserted 1,486 episode rows under new hashes, and queued the entire
back catalogue for download:

```
./gopodder 2026/07/09 09:55:20 db.go:321: Free Thinking is not in the db and seems to be a new podcast, adding
1510 podcasts are in the feeds which have not been downloaded after fn title scan
```

31 duplicate files (~1.6 GB) landed before cron.sh was killed. The 2026-07-05
guid guard in skip.go never fired because it groups by (podcast, guid), and
the podcast title is exactly what changed; the either-hash twin check never
fired because BBC download URLs are signed/rotating (only 74/1,486 rows
matched by `file_url_hash`). The item guids themselves were stable across the
rename — 1,194/1,486 identical.

Fixes:

- `db.go`: rename detection at parse time — a feed title not in the db is
  only a new podcast if its episode guids don't overwhelmingly (≥3 and ≥half)
  belong to one existing podcast; otherwise the podcast is renamed in place
  (`podcasts.title`, `episodes.podcast_title`), episode hashes deliberately
  untouched since the hash is what ties a row to its file on disk.
- `db.go`: on an episode-hash miss, a corroborated same-(podcast, guid)
  sibling (same published date or materially overlapping title) is refreshed
  in place instead of inserting a twin row.
- `skip.go`: download-time backstop — a downloaded same-guid episode of
  ANOTHER podcast with a corroborating title marks the row as a rename
  duplicate.

Verified by replaying the pre-run state (incident db + the real feed):
rename detected (1205/1525 guids), 1,194 episodes refreshed in place, 312
inserted (stale-guid repeats + genuinely new), 266 of those covered by the
same-date/title rule at download time. Residual worst case ~46 episodes vs
1,486.

Cleanup: `cleanup_free_thinking_rename.sh` (run on the gopodder host after
deploying the fix) purges the mistaken "Free Thinking" rows so the next
parse applies the rename, and deletes the 31 duplicate files.

### Residual: repeats re-downloaded after the rename fix (same day)

The 10:59 run applied the rename correctly but still queued 45 episodes: 26
were BBC repeats — same episode title, but a NEW guid and the re-broadcast
date, so neither the guid fallback (guid changed) nor the same-date skip
rule (date changed) caught them; 19 were 2011-era episodes genuinely new to
the collection. 10 duplicate files landed before cron.sh was killed again.

Pre-rename this case could not exist: the episode hash is md5(podcast +
title), so a same-(podcast, title) feed item always collapsed into the
existing row at ingestion. The rename split that identity for every
pre-rename row, so BBC repeats would have kept trickling in as "new"
downloads indefinitely. Fixes, both provably no more aggressive than the
pre-rename hash identity:

- `db.go`: ingestion fallback extended — hash miss, no guid sibling, but a
  same-(podcast, exact title) sibling exists → refresh that row in place.
- `skip.go` rule 3b: an already-downloaded same-podcast episode with the
  identical raw title skips, any date.

Verified by replaying the post-rename live db + real feed: after dropping
the 292 exact-title twin rows, re-ingesting the feed mints zero rows (1,505
matched by guid or title). Cleanup: `cleanup_free_thinking_residual.sh`
drops the twin rows, deletes the 10 duplicate files, and keeps the genuinely
new ones. Expect ~17 legitimate Free Thinking downloads on the next run
(back-catalogue episodes never held).

## Retitled episodes re-downloaded (2026-07-05)

The 18:05 run downloaded four "new" Knowledge Project episodes dated March–April 2026:

```
./gopodder 2026/07/05 18:06:04 gopodder.go:430: Pods for download are
The_Knowledge_Project-2026-04-22-Ai_Goes_Parabolic_OpenAI_Co_Founder_Greg_Brockman-abc3f058....mp3
The_Knowledge_Project-2026-04-14-The_Magic_of_Thinking_Big_Xpo_Ceo_Mario_Harik-0a339d3c....mp3
...
```

All four were retitles of episodes already on disk under 3–5 other names each (The Knowledge Project retitles episodes repeatedly; ~900MB of duplicates in this run alone). The episode hash is an MD5 of podcast title + episode title, so every retitle mints a new hash, and the prefix twin backstop only catches identical titles.

The db already held the answer: the feed `guid` is stable across every retitle, 100% populated (19,699/19,699 rows), and same-(podcast, guid) groups across the whole db were all confirmed retitles (~340 redundant rows over 31 shows; The Econoclasts alone had 128).

Fixes:

- `skip.go`: download-time guard — skip when a same-(podcast, guid) sibling already has a file, or (for guid-rotating feeds) same podcast + date + materially overlapping title, digit-guarded so "Part 1"/"Part 2" and same-day daily-feed episodes never merge. Skips are recorded in the new `skipped_episodes` table.
- `--dedup-guid` / `--dedup-guid-delete`: cleanup pass merging the historical copies (dry-run plan on 2026-07-05: 318 duplicates + 33 stubs, 24 GiB reclaimable, 6 MANUAL holds for retitles that also shifted the published date).

Verified by replaying the pre-run state: the guard skips exactly the four duplicates and still downloads the genuinely new episode from the same run.

## `NULL` `podcast_title`

Somehow adding an empty new podcast

```
./gopodder 2025/06/20 01:01:46 gopodder.go:689: Parsing https://feeds.acast.com/public/shows/6478a825654260001190a7cb
./gopodder 2025/06/20 01:01:46 gopodder.go:508:  is not in the db and seems to be a new podcast, adding
./gopodder 2025/06/20 01:01:48 gopodder.go:689: Parsing https://feeds.acast.com/public/shows/7144a390-7a86-440e-9b2e-db712c18368c
./gopodder 2025/06/20 01:02:06 gopodder.go:689: Parsing https://feeds.acast.com/public/shows/73fe3ede-5c5c-4850-96a8-30db8dbae8bf
./gopodder 2025/06/20 01:02:07 gopodder.go:508:  is not in the db and seems to be a new podcast, adding
```

Taking <https://feeds.acast.com/public/shows/73fe3ede-5c5c-4850-96a8-30db8dbae8bf> this is `FT News Briefing`

Then when running:

```
./gopodder 2025/06/20 01:10:43 gopodder.go:119: sql: Scan error on column index 0, name "podcast_title": converting NULL to string is unsupported (called from: 937)
```

a.k.a

```
./gopodder 2025/07/18 17:07:47 utils.go:35: sql: Scan error on column index 0, name "podcast_title": converting NULL to string is unsupported (called from: 177)
```

```
sqlite> select * from podcasts where title is null;
||||||2023-10-16T19:55:06+01:00|2023-10-16T19:55:06+01:00
||||||2025-06-20T01:00:00+01:00|2025-06-20T01:00:00+01:00
||||||2025-06-20T01:00:00+01:00|2025-06-20T01:00:00+01:00
sqlite> delete from podcasts where title is null;
```

Looks like there are various episodes where podcast_title is indeed null

```
sqlite> select count(*) from episodes where podcast_title is null;
2081
```

After removing these it looks like we're off the the races

Have added some validation around `podcast_title` to make sure it's a sensible string -- will bork if not

To check everything is hunky-dory  in future we might run

```sql
select * from podcasts where title is null;
select * from episodes where podcast_title is null;
```
