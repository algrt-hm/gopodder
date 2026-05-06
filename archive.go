package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	mapset "github.com/deckarep/golang-set"
	_ "github.com/mattn/go-sqlite3"
)

// archiveCandidatesInDir lists basenames in dir that match the gopodder
// filename grammar (5 dashes, contains mp3, not a macOS resource fork).
// Returns the original ReadDir error verbatim if the dir cannot be read.
func archiveCandidatesInDir(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if len(name) >= 2 && name[:2] == "._" {
			continue
		}
		if strings.Count(name, "-") != 5 || !strings.Contains(name, mp3) {
			continue
		}
		out = append(out, name)
	}
	return out, nil
}

// registerArchiveDir scans dir for podcast files matching the gopodder
// filename grammar and inserts/updates rows in archived_episodes so that
// gopodder will not try to re-download them, even if dir is unmounted.
// Returns the number of episodes registered (or refreshed).
func registerArchiveDir(dir string) (int, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return 0, err
	}

	names, err := archiveCandidatesInDir(absDir)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", absDir, err)
	}

	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		return 0, err
	}
	defer db.Close()

	stmt, err := db.Prepare(`
		INSERT INTO archived_episodes
			(podcastname_episodename_hash, archived_path, archived_at)
		VALUES (?, ?, ?)
		ON CONFLICT(podcastname_episodename_hash) DO UPDATE SET
			archived_path = excluded.archived_path,
			archived_at = excluded.archived_at
		;`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	count := 0
	for _, name := range names {
		hash, _, err := hashFromFilename(name)
		if err != nil {
			log.Printf("skipping %s: %v", name, err)
			continue
		}
		fullPath := filepath.Join(absDir, name)
		if _, err := stmt.Exec(hash, fullPath, ts); err != nil {
			return count, fmt.Errorf("upsert %s: %w", name, err)
		}
		count++
	}
	return count, nil
}

// unregisterArchiveDir deletes archive rows whose hash corresponds to a file
// in dir. Useful when moving files back into the primary podcasts directory.
// Returns the number of rows actually removed.
func unregisterArchiveDir(dir string) (int, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return 0, err
	}
	names, err := archiveCandidatesInDir(absDir)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", absDir, err)
	}

	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		return 0, err
	}
	defer db.Close()

	stmt, err := db.Prepare(`DELETE FROM archived_episodes WHERE podcastname_episodename_hash = ?;`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	removed := 0
	for _, name := range names {
		hash, _, err := hashFromFilename(name)
		if err != nil {
			log.Printf("skipping %s: %v", name, err)
			continue
		}
		res, err := stmt.Exec(hash)
		if err != nil {
			return removed, fmt.Errorf("delete %s: %w", name, err)
		}
		affected, _ := res.RowsAffected()
		if affected > 0 {
			removed++
		}
	}
	return removed, nil
}

// reconcileArchiveRegistry removes archived_episodes rows whose archived_path
// no longer resolves on disk. Useful when files have been moved or deleted
// out from under the registry.
func reconcileArchiveRegistry() (int, error) {
	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		return 0, err
	}
	defer db.Close()

	rows, err := db.Query(`
		SELECT podcastname_episodename_hash, archived_path
		FROM archived_episodes
		WHERE archived_path IS NOT NULL AND archived_path != ''
		;`)
	if err != nil {
		return 0, err
	}

	type stale struct {
		hash, path string
	}
	var toDelete []stale
	for rows.Next() {
		var hash, path string
		if err := rows.Scan(&hash, &path); err != nil {
			rows.Close()
			return 0, err
		}
		if _, err := os.Stat(path); err != nil {
			if os.IsNotExist(err) {
				toDelete = append(toDelete, stale{hash, path})
			} else {
				log.Printf("stat %s: %v (keeping registry row)", path, err)
			}
		}
	}
	rows.Close()

	if len(toDelete) == 0 {
		return 0, nil
	}

	stmt, err := db.Prepare(`DELETE FROM archived_episodes WHERE podcastname_episodename_hash = ?;`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	for _, s := range toDelete {
		if _, err := stmt.Exec(s.hash); err != nil {
			return 0, err
		}
		log.Printf("removed stale archive row: %s -> %s", s.hash, s.path)
	}
	return len(toDelete), nil
}

// fetchArchivedHashes returns the set of episode hashes currently registered
// in archived_episodes. Returns an empty set if the table doesn't exist yet.
func fetchArchivedHashes() mapset.Set {
	out := mapset.NewSet()
	db, err := sql.Open(sqlite3, dbFileName)
	checkErr(err)
	if err == nil {
		defer db.Close()
	}
	rows, err := db.Query(`SELECT podcastname_episodename_hash FROM archived_episodes;`)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "no such table") {
			return out
		}
		checkErr(err)
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			checkErr(err)
		}
		out.Add(hash)
	}
	return out
}
