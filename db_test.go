package main

import (
	"crypto/md5"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestNullPodcastTitleCleanup(t *testing.T) {
	useTempWorkingDir(t)
	createTablesIfNotExist()

	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	// Insert rows with NULL and empty podcast_title into episodes
	_, err = db.Exec(`INSERT INTO episodes (podcast_title, title, first_seen, last_seen, podcastname_episodename_hash) VALUES (NULL, 'Ep Null', ?, ?, 'hash-null')`, ts, ts)
	if err != nil {
		t.Fatalf("insert NULL row: %v", err)
	}
	_, err = db.Exec(`INSERT INTO episodes (podcast_title, title, first_seen, last_seen, podcastname_episodename_hash) VALUES ('', 'Ep Empty', ?, ?, 'hash-empty')`, ts, ts)
	if err != nil {
		t.Fatalf("insert empty row: %v", err)
	}
	_, err = db.Exec(`INSERT INTO episodes (podcast_title, title, first_seen, last_seen, podcastname_episodename_hash) VALUES ('  ', 'Ep Spaces', ?, ?, 'hash-spaces')`, ts, ts)
	if err != nil {
		t.Fatalf("insert whitespace-only row: %v", err)
	}

	// Insert a valid row that should survive cleanup
	_, err = db.Exec(`INSERT INTO episodes (podcast_title, title, first_seen, last_seen, podcastname_episodename_hash) VALUES ('Good Podcast', 'Ep Good', ?, ?, 'hash-good')`, ts, ts)
	if err != nil {
		t.Fatalf("insert good row: %v", err)
	}

	// Same for interactive_episodes (podcast_title is NOT NULL so we can only test empty/spaces)
	_, err = db.Exec(`INSERT INTO interactive_episodes (podcast_title, title, first_seen, last_seen, podcastname_episodename_hash) VALUES ('', 'IE Empty', ?, ?, 'ie-hash-empty')`, ts, ts)
	if err != nil {
		t.Fatalf("insert interactive empty row: %v", err)
	}
	_, err = db.Exec(`INSERT INTO interactive_episodes (podcast_title, title, first_seen, last_seen, podcastname_episodename_hash) VALUES ('  ', 'IE Spaces', ?, ?, 'ie-hash-spaces')`, ts, ts)
	if err != nil {
		t.Fatalf("insert interactive spaces row: %v", err)
	}
	_, err = db.Exec(`INSERT INTO interactive_episodes (podcast_title, title, first_seen, last_seen, podcastname_episodename_hash) VALUES ('Good Interactive', 'IE Good', ?, ?, 'ie-hash-good')`, ts, ts)
	if err != nil {
		t.Fatalf("insert interactive good row: %v", err)
	}

	db.Close()

	// Run createTablesIfNotExist again — this triggers the cleanup migration
	createTablesIfNotExist()

	// Verify bad rows are gone, good rows remain
	db, err = sql.Open(sqlite3, dbFileName)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer db.Close()

	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM episodes`).Scan(&count)
	if err != nil {
		t.Fatalf("count episodes: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 episode row after cleanup, got %d", count)
	}

	var survivingTitle string
	err = db.QueryRow(`SELECT podcast_title FROM episodes`).Scan(&survivingTitle)
	if err != nil {
		t.Fatalf("query surviving episode: %v", err)
	}
	if survivingTitle != "Good Podcast" {
		t.Fatalf("expected surviving title %q, got %q", "Good Podcast", survivingTitle)
	}

	err = db.QueryRow(`SELECT COUNT(*) FROM interactive_episodes`).Scan(&count)
	if err != nil {
		t.Fatalf("count interactive_episodes: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 interactive_episodes row after cleanup, got %d", count)
	}

	err = db.QueryRow(`SELECT podcast_title FROM interactive_episodes`).Scan(&survivingTitle)
	if err != nil {
		t.Fatalf("query surviving interactive episode: %v", err)
	}
	if survivingTitle != "Good Interactive" {
		t.Fatalf("expected surviving title %q, got %q", "Good Interactive", survivingTitle)
	}
}

func TestUpdateDatabaseForDownloadsNormalizesExistingHash(t *testing.T) {
	tmpDir := useTempWorkingDir(t)
	createTablesIfNotExist()

	podcastTitle := "Repair Show"
	episodeTitle := "Repair Episode"
	filename := buildEpisodeFilename(podcastTitle, episodeTitle, "2024-01-02")
	fullPath := filepath.Join(tmpDir, filename)
	if err := os.WriteFile(fullPath, []byte("mp3"), 0666); err != nil {
		t.Fatalf("write podcast file: %v", err)
	}

	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	oldHash := fmt.Sprintf("%x", md5.Sum([]byte("https://example.com/audio.mp3")))
	_, err = db.Exec(`
		INSERT INTO downloads (filename, hash, first_seen, last_seen, tagged_at)
		VALUES (?, ?, ?, ?, NULL)
		;`,
		filename, oldHash, "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z",
	)
	if err != nil {
		t.Fatalf("insert downloads row: %v", err)
	}

	updateDatabaseForDownloads()

	wantHash, _, err := hashFromFilename(filename)
	if err != nil {
		t.Fatalf("hashFromFilename: %v", err)
	}

	var gotHash string
	if err := db.QueryRow(`SELECT hash FROM downloads WHERE filename = ?;`, filename).Scan(&gotHash); err != nil {
		t.Fatalf("query downloads row: %v", err)
	}
	if gotHash != wantHash {
		t.Fatalf("got hash %q, want %q", gotHash, wantHash)
	}
}

// renameTestFeed builds the pod map and episode list for a feed titled
// podTitle whose episodes carry stable guids — the shape of a feed before and
// after a publisher renames the show.
func renameTestFeed(podTitle string, n int) (map[string]string, []M) {
	pod := map[string]string{"title": podTitle, "author": "a", "category": "c",
		"description": "d", "language": "l", "link": "https://example.com/" + podTitle}
	episodes := make([]M, 0, n)
	for i := 0; i < n; i++ {
		episodes = append(episodes, M{
			"title":     fmt.Sprintf("Episode number %d with a distinctive name", i),
			"guid":      fmt.Sprintf("urn:test:guid-%d", i),
			"published": fmt.Sprintf("2026-06-%02dT10:00:00Z", i+1),
			"file":      fmt.Sprintf("https://example.com/audio/%d.mp3", i),
		})
	}
	return pod, episodes
}

// A feed whose title is unknown but whose guids overwhelmingly belong to an
// existing podcast is that podcast renamed (the 2026-07-09 "Arts & Ideas" →
// "Free Thinking" incident). The rename must happen in place: one podcasts
// row, podcast_title rewritten, and — critically — episode hashes untouched,
// because the hash is what ties a row to its file on disk.
func TestPodcastRenameUpdatesInPlace(t *testing.T) {
	useTempWorkingDir(t)
	createTablesIfNotExist()

	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	oldPod, episodes := renameTestFeed("Arts & Ideas", 5)
	podEpisodesIntoDatabase(db, oldPod, episodes)

	newPod, episodes := renameTestFeed("Free Thinking", 5)
	// One genuinely new episode alongside the renamed back catalogue
	episodes = append(episodes, M{"title": "A brand new episode about rocks",
		"guid": "urn:test:guid-new", "published": "2026-07-09T10:00:00Z",
		"file": "https://example.com/audio/new.mp3"})
	podEpisodesIntoDatabase(db, newPod, episodes)

	var podCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM podcasts;`).Scan(&podCount); err != nil {
		t.Fatalf("count podcasts: %v", err)
	}
	if podCount != 1 {
		t.Fatalf("expected 1 podcasts row after rename, got %d", podCount)
	}
	var podTitle string
	if err := db.QueryRow(`SELECT title FROM podcasts;`).Scan(&podTitle); err != nil {
		t.Fatalf("query podcast title: %v", err)
	}
	if podTitle != "Free Thinking" {
		t.Fatalf("podcasts row title = %q, want Free Thinking", podTitle)
	}

	var epCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM episodes;`).Scan(&epCount); err != nil {
		t.Fatalf("count episodes: %v", err)
	}
	if epCount != 6 {
		t.Fatalf("expected 6 episode rows (5 renamed + 1 new), got %d", epCount)
	}

	// The back catalogue keeps its old-title hashes under the new name
	for i := 0; i < 5; i++ {
		epTitle := fmt.Sprintf("Episode number %d with a distinctive name", i)
		oldHash := fmt.Sprintf("%x", md5.Sum([]byte("Arts & Ideas"+epTitle)))
		var gotPod string
		if err := db.QueryRow(`SELECT podcast_title FROM episodes WHERE podcastname_episodename_hash=?;`, oldHash).Scan(&gotPod); err != nil {
			t.Fatalf("episode %d lost its original hash %s: %v", i, oldHash, err)
		}
		if gotPod != "Free Thinking" {
			t.Fatalf("episode %d podcast_title = %q, want Free Thinking", i, gotPod)
		}
	}

	// The genuinely new episode hashes under the new podcast name
	newHash := fmt.Sprintf("%x", md5.Sum([]byte("Free Thinking"+"A brand new episode about rocks")))
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM episodes WHERE podcastname_episodename_hash=?;`, newHash).Scan(&n); err != nil || n != 1 {
		t.Fatalf("new episode not inserted under new-name hash: n=%d err=%v", n, err)
	}
}

// A new feed that happens to share a couple of guids with an existing podcast
// is NOT a rename — below the match threshold it must be added as a new
// podcast, exactly as before.
func TestPodcastRenameNotDetectedBelowThreshold(t *testing.T) {
	useTempWorkingDir(t)
	createTablesIfNotExist()

	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	oldPod, oldEpisodes := renameTestFeed("Some Established Show", 6)
	podEpisodesIntoDatabase(db, oldPod, oldEpisodes)

	newPod, newEpisodes := renameTestFeed("A Different Show Entirely", 6)
	// Overlap on 2 of 6 guids: under renameMinGuidMatches and under half
	for i := 2; i < 6; i++ {
		newEpisodes[i]["guid"] = fmt.Sprintf("urn:test:other-%d", i)
	}
	podEpisodesIntoDatabase(db, newPod, newEpisodes)

	var podCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM podcasts;`).Scan(&podCount); err != nil {
		t.Fatalf("count podcasts: %v", err)
	}
	if podCount != 2 {
		t.Fatalf("expected 2 podcasts rows (no rename), got %d", podCount)
	}
}

// A retitled episode (same guid, same published date, new title) must refresh
// the existing row instead of inserting a twin that would be re-downloaded.
func TestEpisodeRetitleRefreshedByGuidNoTwinRow(t *testing.T) {
	useTempWorkingDir(t)
	createTablesIfNotExist()

	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	pod, episodes := renameTestFeed("The Knowledge Project", 4)
	podEpisodesIntoDatabase(db, pod, episodes)

	// Feed retitles episode 0, keeping guid and published date
	episodes[0]["title"] = "A completely rewritten marketing title"
	podEpisodesIntoDatabase(db, pod, episodes)

	var epCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM episodes;`).Scan(&epCount); err != nil {
		t.Fatalf("count episodes: %v", err)
	}
	if epCount != 4 {
		t.Fatalf("expected 4 episode rows (retitle refreshed in place), got %d", epCount)
	}
	origHash := fmt.Sprintf("%x", md5.Sum([]byte("The Knowledge Project"+"Episode number 0 with a distinctive name")))
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM episodes WHERE podcastname_episodename_hash=?;`, origHash).Scan(&n); err != nil || n != 1 {
		t.Fatalf("retitled episode lost its original row: n=%d err=%v", n, err)
	}
}

// Two same-day episodes of a feed that reuses one junk guid must both be
// inserted when their titles don't corroborate a retitle... unless the date
// corroborates. Same date + same guid IS treated as a retitle (matching the
// trust skip.go already places in same-podcast guids), so distinct episodes
// sharing a guid must differ in published date to both get rows.
func TestEpisodeGuidFallbackDistinctEpisodesDifferentDates(t *testing.T) {
	useTempWorkingDir(t)
	createTablesIfNotExist()

	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	pod := map[string]string{"title": "Junk Guid Daily", "author": "a", "category": "c",
		"description": "d", "language": "l", "link": "k"}
	episodes := []M{
		{"title": "Markets slide on tariff news", "guid": "1",
			"published": "2026-07-01T06:00:00Z", "file": "https://x/1.mp3"},
	}
	podEpisodesIntoDatabase(db, pod, episodes)

	episodes = []M{
		{"title": "An interview about gardening", "guid": "1",
			"published": "2026-07-02T06:00:00Z", "file": "https://x/2.mp3"},
	}
	podEpisodesIntoDatabase(db, pod, episodes)

	var epCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM episodes;`).Scan(&epCount); err != nil {
		t.Fatalf("count episodes: %v", err)
	}
	if epCount != 2 {
		t.Fatalf("expected 2 episode rows for distinct junk-guid episodes, got %d", epCount)
	}
}

// A repeat re-entering the feed with a fresh guid AND a new published date
// (the re-broadcast) must collapse into the existing row by exact title —
// the identity the episode hash provided before podcast renames split it.
func TestEpisodeRepeatNewGuidNewDateRefreshedByTitle(t *testing.T) {
	useTempWorkingDir(t)
	createTablesIfNotExist()

	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	pod := map[string]string{"title": "Free Thinking", "author": "a", "category": "c",
		"description": "d", "language": "l", "link": "k"}
	episodes := []M{{"title": "Hitchhiking", "guid": "urn:bbc:podcast:p0bpr86s",
		"published": "2023-06-15T16:00:00Z", "file": "https://x/orig.mp3"}}
	podEpisodesIntoDatabase(db, pod, episodes)

	// Same episode repeated: new guid, new date, identical title
	episodes = []M{{"title": "Hitchhiking", "guid": "urn:bbc:podcast:p0hffcg6",
		"published": "2024-02-28T17:00:00Z", "file": "https://x/repeat.mp3"}}
	podEpisodesIntoDatabase(db, pod, episodes)

	var epCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM episodes;`).Scan(&epCount); err != nil {
		t.Fatalf("count episodes: %v", err)
	}
	if epCount != 1 {
		t.Fatalf("expected repeat to refresh the existing row, got %d rows", epCount)
	}
	origHash := fmt.Sprintf("%x", md5.Sum([]byte("Free Thinking"+"Hitchhiking")))
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM episodes WHERE podcastname_episodename_hash=?;`, origHash).Scan(&n); err != nil || n != 1 {
		t.Fatalf("repeat did not keep the original row identity: n=%d err=%v", n, err)
	}
}
