package main

import (
	"database/sql"
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
