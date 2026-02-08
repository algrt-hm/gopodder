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
	firstSeen := "2024-02-03T01:02:03Z"

	got := episodeTimestamp(published, firstSeen, now)
	want, err := time.Parse(time.RFC3339, published)
	if err != nil {
		t.Fatalf("parse published: %v", err)
	}
	if !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	got = episodeTimestamp("", firstSeen, now)
	want, err = time.Parse(time.RFC3339, firstSeen)
	if err != nil {
		t.Fatalf("parse firstSeen: %v", err)
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

func TestLoadEpisodeItemsUsesEpisodesFirstSeen(t *testing.T) {
	tmpDir := useTempWorkingDir(t)
	_ = tmpDir
	createTablesIfNotExist()

	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	podTitle := "DB Podcast"
	episodeTitle := "No Published"
	fileURL := "https://example.com/no-pub.mp3"
	hash := "hash-no-published"
	fileURLHash := "fhash-no-published"
	interactiveFirstSeen := "2024-02-01T01:02:03Z"
	episodesFirstSeen := "2024-01-05T10:11:12Z"

	_, err = db.Exec(`
		INSERT INTO interactive_episodes (
			podcast_title, title, published, file, first_seen, last_seen,
			podcastname_episodename_hash, file_url_hash
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		;`,
		podTitle,
		episodeTitle,
		"",
		fileURL,
		interactiveFirstSeen,
		interactiveFirstSeen,
		hash,
		fileURLHash,
	)
	if err != nil {
		t.Fatalf("insert interactive row: %v", err)
	}

	_, err = db.Exec(`
		INSERT INTO episodes (
			podcast_title, title, published, file, first_seen, last_seen,
			podcastname_episodename_hash, file_url_hash
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		;`,
		podTitle,
		episodeTitle,
		"",
		fileURL,
		episodesFirstSeen,
		episodesFirstSeen,
		hash,
		fileURLHash,
	)
	if err != nil {
		t.Fatalf("insert episodes row: %v", err)
	}

	items, err := loadEpisodeItemsFromDatabase(podTitle)
	if err != nil {
		t.Fatalf("loadEpisodeItemsFromDatabase error: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("got %d items, want 1", len(items))
	}

	expectedFilename := buildEpisodeFilenameWithHash(podTitle, episodeTitle, "2024-01-05", hash)
	if items[0].filename != expectedFilename {
		t.Fatalf("got filename %q, want %q", items[0].filename, expectedFilename)
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

	wantHash, _, err := hashFromFilename(baseFilename)
	if err != nil {
		t.Fatalf("hashFromFilename: %v", err)
	}
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

func TestLoadEpisodeItemsDownloadedFlag(t *testing.T) {
	_ = useTempWorkingDir(t)
	createTablesIfNotExist()

	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	// Insert two episodes
	for _, ep := range []struct {
		title string
		hash  string
	}{
		{"Downloaded Ep", "hash-dl"},
		{"Not Downloaded Ep", "hash-nodl"},
	} {
		_, err := db.Exec(`
			INSERT INTO interactive_episodes (
				podcast_title, title, published, file, first_seen, last_seen,
				podcastname_episodename_hash, file_url_hash
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			;`,
			"DL Podcast", ep.title, "2024-01-01T01:00:00Z",
			"https://example.com/"+ep.hash+".mp3",
			ts, ts, ep.hash, "f"+ep.hash,
		)
		if err != nil {
			t.Fatalf("insert episode: %v", err)
		}
	}

	// Record one as downloaded
	_, err = db.Exec(`
		INSERT INTO downloads (filename, hash, first_seen, last_seen)
		VALUES (?, ?, ?, ?)
		;`, "dl-podcast-2024-01-01-downloaded-ep-hash-dl.mp3", "hash-dl", ts, ts)
	if err != nil {
		t.Fatalf("insert download: %v", err)
	}

	items, err := loadEpisodeItemsFromDatabase("DL Podcast")
	if err != nil {
		t.Fatalf("loadEpisodeItemsFromDatabase error: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("got %d items, want 2", len(items))
	}

	dlCount := 0
	noDlCount := 0
	for _, item := range items {
		if item.title == "Downloaded Ep" {
			if !item.downloaded {
				t.Fatalf("expected Downloaded Ep to have downloaded=true")
			}
			dlCount++
		}
		if item.title == "Not Downloaded Ep" {
			if item.downloaded {
				t.Fatalf("expected Not Downloaded Ep to have downloaded=false")
			}
			noDlCount++
		}
	}
	if dlCount != 1 || noDlCount != 1 {
		t.Fatalf("unexpected item counts: dl=%d, nodl=%d", dlCount, noDlCount)
	}
}

func TestRebuildVisibleItems(t *testing.T) {
	allItems := []episodeItem{
		{title: "Ep1", filename: "ep1.mp3", downloaded: false},
		{title: "Ep2", filename: "ep2.mp3", downloaded: true},
		{title: "Ep3", filename: "ep3.mp3", downloaded: false},
	}
	m := interactiveModel{
		allItems:   allItems,
		items:      append([]episodeItem{}, allItems...),
		windowSize: 10,
	}

	// Without filter, all items visible
	m.hideDownloaded = false
	m.rebuildVisibleItems()
	if len(m.items) != 3 {
		t.Fatalf("expected 3 visible items, got %d", len(m.items))
	}

	// Select Ep1 and Ep3 in visible items
	m.items[0].selected = true
	m.items[2].selected = true

	// Enable filter
	m.hideDownloaded = true
	m.rebuildVisibleItems()
	if len(m.items) != 2 {
		t.Fatalf("expected 2 visible items with filter, got %d", len(m.items))
	}
	for _, item := range m.items {
		if item.downloaded {
			t.Fatalf("downloaded item %q should not be visible", item.title)
		}
	}

	// Check selections are preserved
	ep1Found := false
	ep3Found := false
	for _, item := range m.items {
		if item.title == "Ep1" && item.selected {
			ep1Found = true
		}
		if item.title == "Ep3" && item.selected {
			ep3Found = true
		}
	}
	if !ep1Found {
		t.Fatalf("Ep1 selection should be preserved after rebuild")
	}
	if !ep3Found {
		t.Fatalf("Ep3 selection should be preserved after rebuild")
	}

	// Toggle back, selections preserved in allItems
	m.hideDownloaded = false
	m.rebuildVisibleItems()
	if len(m.items) != 3 {
		t.Fatalf("expected 3 visible items after unfilter, got %d", len(m.items))
	}
	// Verify Ep1 and Ep3 still selected after round-trip
	for _, item := range m.items {
		if item.title == "Ep1" && !item.selected {
			t.Fatalf("Ep1 should still be selected after round-trip")
		}
		if item.title == "Ep3" && !item.selected {
			t.Fatalf("Ep3 should still be selected after round-trip")
		}
	}
}

func TestDownloadedCount(t *testing.T) {
	m := interactiveModel{
		allItems: []episodeItem{
			{title: "A", downloaded: true},
			{title: "B", downloaded: false},
			{title: "C", downloaded: true},
			{title: "D", downloaded: false},
		},
	}
	got := m.downloadedCount()
	if got != 2 {
		t.Fatalf("expected downloadedCount()=2, got %d", got)
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
