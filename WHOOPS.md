# Bugs found

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
