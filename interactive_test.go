package main

import (
	"crypto/md5"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEpisodeTimestamp(t *testing.T) {
	now := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	published := "2024-01-02T10:11:12Z"
	updated := "2024-02-03T01:02:03Z"

	got := episodeTimestamp(published, updated, now)
	want, err := time.Parse(time.RFC3339, published)
	if err != nil {
		t.Fatalf("parse published: %v", err)
	}
	if !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	got = episodeTimestamp("", updated, now)
	want, err = time.Parse(time.RFC3339, updated)
	if err != nil {
		t.Fatalf("parse updated: %v", err)
	}
	if !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	got = episodeTimestamp("", "", now)
	if !got.Equal(now) {
		t.Fatalf("got %v, want %v", got, now)
	}
}

func TestBuildEpisodeFilename(t *testing.T) {
	podcastTitle := "My Podcast"
	episodeTitle := "Hello World"
	dateStr := "2024-01-02"

	hash := fmt.Sprintf("%x", md5.Sum([]byte(podcastTitle+episodeTitle)))
	want := fmt.Sprintf(
		"%s-%s-%s-%s.%s",
		titleTransformation(podcastTitle),
		dateStr,
		titleTransformation(episodeTitle),
		hash,
		mp3,
	)

	got := buildEpisodeFilename(podcastTitle, episodeTitle, dateStr)
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestExpandHome(t *testing.T) {
	t.Setenv("HOME", "/tmp/home")

	got := expandHome("~")
	if got != "/tmp/home" {
		t.Fatalf("got %q, want %q", got, "/tmp/home")
	}

	got = expandHome("~/pods")
	if got != "/tmp/home/pods" {
		t.Fatalf("got %q, want %q", got, "/tmp/home/pods")
	}

	got = expandHome("/var/tmp")
	if got != "/var/tmp" {
		t.Fatalf("got %q, want %q", got, "/var/tmp")
	}

	got = expandHome("~other")
	if got != "~other" {
		t.Fatalf("got %q, want %q", got, "~other")
	}
}

func TestLoadExtraFeeds(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, extraConfName)
	content := "https://example.com/feed.rss\nnot a url\nhttp://example.org/other.xml\n"
	if err := os.WriteFile(path, []byte(content), 0666); err != nil {
		t.Fatalf("write extra conf: %v", err)
	}

	gotPath, gotFeeds, err, exists := loadExtraFeeds(dir)
	if err != nil {
		t.Fatalf("loadExtraFeeds error: %v", err)
	}
	if !exists {
		t.Fatalf("expected exists true")
	}
	if gotPath != path {
		t.Fatalf("got path %q, want %q", gotPath, path)
	}
	if len(gotFeeds) != 2 {
		t.Fatalf("got %d feeds, want 2", len(gotFeeds))
	}

	_, _, err, exists = loadExtraFeeds(filepath.Join(dir, "missing"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exists {
		t.Fatalf("expected exists false")
	}
}

func TestLoadPodcastTitlesFromDatabase(t *testing.T) {
	tmpDir := useTempWorkingDir(t)
	_ = tmpDir
	createTablesIfNotExist()

	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	rows := []struct {
		podcastTitle string
		episodeTitle string
		hash         string
		fileURLHash  string
	}{
		{podcastTitle: "Zulu Cast", episodeTitle: "E1", hash: "hash-z-1", fileURLHash: "fhash-z-1"},
		{podcastTitle: "Alpha Cast", episodeTitle: "E2", hash: "hash-a-2", fileURLHash: "fhash-a-2"},
		{podcastTitle: "Zulu Cast", episodeTitle: "E3", hash: "hash-z-3", fileURLHash: "fhash-z-3"},
	}

	for _, row := range rows {
		_, err := db.Exec(`
			INSERT INTO interactive_episodes (
				podcast_title, title, file, first_seen, last_seen,
				podcastname_episodename_hash, file_url_hash
			) VALUES (?, ?, ?, ?, ?, ?, ?)
			;`,
			row.podcastTitle,
			row.episodeTitle,
			"https://example.com/"+row.hash+".mp3",
			ts,
			ts,
			row.hash,
			row.fileURLHash,
		)
		if err != nil {
			t.Fatalf("insert row: %v", err)
		}
	}

	gotTitles, err := loadPodcastTitlesFromDatabase()
	if err != nil {
		t.Fatalf("loadPodcastTitlesFromDatabase error: %v", err)
	}

	wantTitles := []string{"Alpha Cast", "Zulu Cast"}
	if len(gotTitles) != len(wantTitles) {
		t.Fatalf("got %d titles, want %d (%v)", len(gotTitles), len(wantTitles), gotTitles)
	}
	for i := range wantTitles {
		if gotTitles[i] != wantTitles[i] {
			t.Fatalf("got titles %v, want %v", gotTitles, wantTitles)
		}
	}
}

func TestLoadEpisodeItemsFromDatabase(t *testing.T) {
	tmpDir := useTempWorkingDir(t)
	_ = tmpDir
	createTablesIfNotExist()

	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	type row struct {
		podcastTitle string
		episodeTitle string
		published    string
		fileURL      string
		hash         string
		fileURLHash  string
	}

	rows := []row{
		{
			podcastTitle: "DB Podcast",
			episodeTitle: "Older Episode",
			published:    "2024-01-01T01:00:00Z",
			fileURL:      "https://example.com/older.mp3",
			hash:         "hash-old",
			fileURLHash:  "fhash-old",
		},
		{
			podcastTitle: "DB Podcast",
			episodeTitle: "Latest Episode",
			published:    "2024-01-03T01:00:00Z",
			fileURL:      "https://example.com/latest.mp3",
			hash:         "hash-latest",
			fileURLHash:  "fhash-latest",
		},
		{
			podcastTitle: "DB Podcast",
			episodeTitle: "No Audio",
			published:    "2024-01-04T01:00:00Z",
			fileURL:      "",
			hash:         "hash-no-file",
			fileURLHash:  "fhash-no-file",
		},
	}

	for _, row := range rows {
		_, err := db.Exec(`
			INSERT INTO interactive_episodes (
				podcast_title, title, published, file, first_seen, last_seen,
				podcastname_episodename_hash, file_url_hash
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			;`,
			row.podcastTitle,
			row.episodeTitle,
			row.published,
			row.fileURL,
			ts,
			ts,
			row.hash,
			row.fileURLHash,
		)
		if err != nil {
			t.Fatalf("insert row: %v", err)
		}
	}

	items, err := loadEpisodeItemsFromDatabase("DB Podcast")
	if err != nil {
		t.Fatalf("loadEpisodeItemsFromDatabase error: %v", err)
	}

	if len(items) != 2 {
		t.Fatalf("got %d items, want 2", len(items))
	}
	if items[0].title != "Latest Episode" {
		t.Fatalf("expected latest episode first, got %q", items[0].title)
	}
	if items[0].url != "https://example.com/latest.mp3" {
		t.Fatalf("unexpected first URL: %q", items[0].url)
	}
	if !strings.HasSuffix(items[0].filename, "-hash-latest.mp3") {
		t.Fatalf("expected filename to use db hash, got %q", items[0].filename)
	}
}

func TestRecordInteractiveDownload(t *testing.T) {
	tmpDir := useTempWorkingDir(t)
	_ = tmpDir
	createTablesIfNotExist()

	baseFilename := buildEpisodeFilename("DB Podcast", "Interactive Download", "2024-01-05")
	fullPath := filepath.Join("/tmp", baseFilename)

	if err := recordInteractiveDownload(fullPath); err != nil {
		t.Fatalf("recordInteractiveDownload first call: %v", err)
	}
	if err := recordInteractiveDownload(fullPath); err != nil {
		t.Fatalf("recordInteractiveDownload second call: %v", err)
	}

	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	var gotFilename, gotHash string
	var gotTaggedAt sql.NullString
	row := db.QueryRow(`
		SELECT filename, hash, tagged_at
		FROM downloads
		WHERE filename = ?
		;`, baseFilename)
	if err := row.Scan(&gotFilename, &gotHash, &gotTaggedAt); err != nil {
		t.Fatalf("query downloads row: %v", err)
	}

	wantHash, transformedTitle := hashFromFilename(baseFilename)
	_ = transformedTitle
	if gotFilename != baseFilename {
		t.Fatalf("got filename %q, want %q", gotFilename, baseFilename)
	}
	if gotHash != wantHash {
		t.Fatalf("got hash %q, want %q", gotHash, wantHash)
	}
	if !gotTaggedAt.Valid || strings.TrimSpace(gotTaggedAt.String) == "" {
		t.Fatalf("expected tagged_at to be set, got %+v", gotTaggedAt)
	}
}

func TestStoreParsedFeedInInteractiveTable(t *testing.T) {
	tmpDir := useTempWorkingDir(t)
	_ = tmpDir
	createTablesIfNotExist()

	pod := map[string]string{
		title: "Stored From Interactive URL",
	}

	episodes := []M{
		{
			title:       "Episode One",
			file:        "https://example.com/ep1.mp3",
			published:   "2024-01-10T01:00:00Z",
			updated:     "2024-01-10T01:00:00Z",
			author:      "Host A",
			description: "<p>desc</p>",
			guid:        "guid-1",
			link:        "https://example.com/ep1",
			format:      "audio/mpeg",
			episode:     "1",
		},
	}

	if err := storeParsedFeedInInteractiveTable(pod, episodes); err != nil {
		t.Fatalf("storeParsedFeedInInteractiveTable error: %v", err)
	}

	titles, err := loadPodcastTitlesFromDatabase()
	if err != nil {
		t.Fatalf("loadPodcastTitlesFromDatabase error: %v", err)
	}

	found := false
	for _, each := range titles {
		if each == pod[title] {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected podcast title %q in %v", pod[title], titles)
	}

	items, err := loadEpisodeItemsFromDatabase(pod[title])
	if err != nil {
		t.Fatalf("loadEpisodeItemsFromDatabase error: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("got %d items, want 1", len(items))
	}
	if items[0].title != "Episode One" {
		t.Fatalf("got episode %q, want Episode One", items[0].title)
	}
}

func useTempWorkingDir(t *testing.T) string {
	t.Helper()
	oldWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}

	dir := t.TempDir()
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir temp dir: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(oldWd); err != nil {
			t.Fatalf("restore wd: %v", err)
		}
	})
	return dir
}
