# Bugs found

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
