package main

import (
	"crypto/md5"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	strip "github.com/grokify/html-strip-tags-go" // Lift of stripTags from html/template package
	_ "github.com/mattn/go-sqlite3"
)

type latestPodResult struct {
	author          sql.NullString
	title           sql.NullString
	published       sql.NullString
	podcast_title   sql.NullString
	dateForFilename sql.NullString
	hash            sql.NullString
	file            sql.NullString
}

// nullStrToStr is a utility function to convert a NullString to a string
func nullStrToStr(s sql.NullString) string {
	if s.Valid {
		return s.String
	} else {
		return "?"
	}
}

// checkStr does a bit of validation on strings
func checkStr(str string, nameOfStr string) {
	// The title needs to have string length after stripping of at least 3 characters otherwise we bail
	strLen := len(strings.TrimSpace(str))

	if strLen < 3 {
		log.Panicf("%s of %s (len %v) doesn't seem sensible, bailing", nameOfStr, str, strLen)
	}
}

// nullWrap is a utility function to convert a string to NullString if empty
// Lifted from: https://stackoverflow.com/questions/40266633/golang-insert-null-into-sql-instead-of-empty-string
func nullWrap(s string) sql.NullString {
	if len(strings.TrimSpace(s)) == 0 {
		return sql.NullString{}
	}
	// implied else
	return sql.NullString{
		String: s,
		Valid:  true,
	}
}

// createTablesIfNotExist creates our SQLite db and tables if they do not exist
func createTablesIfNotExist() {
	/*
		select * from podcasts where title is null;
		select * from episodes where podcast_title is null;
	*/

	createPodcasts := `
	CREATE TABLE IF NOT EXISTS podcasts (
		title TEXT PRIMARY KEY,
		author TEXT,
		description TEXT,
		language TEXT,
		link TEXT,
		category TEXT,
		first_seen TEXT NOT NULL,
		last_seen TEXT NOT NULL
	);
	`

	createEpisodes := `
	CREATE TABLE IF NOT EXISTS episodes (
		author TEXT,
		description TEXT,
		episode INTEGER,
		file TEXT,
		format TEXT,
		guid TEXT,
		link TEXT,
		published TEXT,
		title TEXT,
		updated TEXT,
		-- these are our own internally-generated data
		first_seen TEXT,
		last_seen TEXT,
		podcast_title TEXT, -- we will join on this
		podcastname_episodename_hash TEXT PRIMARY KEY,
		file_url_hash TEXT
	);
	`

	createInteractiveEpisodes := `
	CREATE TABLE IF NOT EXISTS interactive_episodes (
		author TEXT,
		description TEXT,
		episode INTEGER,
		file TEXT,
		format TEXT,
		guid TEXT,
		link TEXT,
		published TEXT,
		title TEXT,
		updated TEXT,
		first_seen TEXT NOT NULL,
		last_seen TEXT NOT NULL,
		podcast_title TEXT NOT NULL,
		podcastname_episodename_hash TEXT PRIMARY KEY,
		file_url_hash TEXT
	);
	`

	createInteractiveEpisodesPodcastIdx := `
	CREATE INDEX IF NOT EXISTS idx_interactive_episodes_podcast_title
	ON interactive_episodes (podcast_title);
	`

	createEpisodesFileUrlHashIdx := `
	CREATE INDEX IF NOT EXISTS idx_episodes_file_url_hash
	ON episodes (file_url_hash);
	`

	// The last_seen refresh in podEpisodesIntoDatabase now updates by primary
	// key, so this index is no longer needed for the parse hot path; kept for
	// ad-hoc title lookups.
	createEpisodesTitleIdx := `
	CREATE INDEX IF NOT EXISTS idx_episodes_title
	ON episodes (title);
	`

	createInteractiveEpisodesFileUrlHashIdx := `
	CREATE INDEX IF NOT EXISTS idx_interactive_episodes_file_url_hash
	ON interactive_episodes (file_url_hash);
	`

	createDownloaded := `
	CREATE TABLE IF NOT EXISTS downloads (
		filename TEXT PRIMARY KEY,
		hash TEXT NOT NULL,
		first_seen TEXT NOT NULL,
		last_seen TEXT NOT NULL,
		tagged_at TEXT DEFAULT NULL
	);
	`

	createArchivedEpisodes := `
	CREATE TABLE IF NOT EXISTS archived_episodes (
		podcastname_episodename_hash TEXT PRIMARY KEY,
		archived_path TEXT,
		archived_at TEXT NOT NULL
	);
	`

	createArchivedEpisodesPathIdx := `
	CREATE INDEX IF NOT EXISTS idx_archived_episodes_path
	ON archived_episodes (archived_path);
	`

	// Audit trail of episodes the download pass refused as retitle duplicates
	// (see skip.go); one row per skipped episode, refreshed on every run that
	// skips it again.
	createSkippedEpisodes := `
	CREATE TABLE IF NOT EXISTS skipped_episodes (
		podcastname_episodename_hash TEXT PRIMARY KEY,
		podcast_title TEXT,
		title TEXT,
		guid TEXT,
		matched_episode_hash TEXT,
		matched_title TEXT,
		reason TEXT,
		first_skipped TEXT NOT NULL,
		last_skipped TEXT NOT NULL
	);
	`

	db, err := sql.Open(sqlite3, dbFileName)
	checkErr(err)

	if err == nil {
		defer db.Close()

		statement, err := db.Prepare(createPodcasts)
		checkErr(err)
		_, err = statement.Exec()
		checkErr(err)

		statement, err = db.Prepare(createEpisodes)
		checkErr(err)
		_, err = statement.Exec()
		checkErr(err)

		statement, err = db.Prepare(createInteractiveEpisodes)
		checkErr(err)
		_, err = statement.Exec()
		checkErr(err)

		statement, err = db.Prepare(createInteractiveEpisodesPodcastIdx)
		checkErr(err)
		_, err = statement.Exec()
		checkErr(err)

		statement, err = db.Prepare(createEpisodesFileUrlHashIdx)
		checkErr(err)
		_, err = statement.Exec()
		checkErr(err)

		statement, err = db.Prepare(createEpisodesTitleIdx)
		checkErr(err)
		_, err = statement.Exec()
		checkErr(err)

		statement, err = db.Prepare(createInteractiveEpisodesFileUrlHashIdx)
		checkErr(err)
		_, err = statement.Exec()
		checkErr(err)

		statement, err = db.Prepare(createDownloaded)
		checkErr(err)
		_, err = statement.Exec()
		checkErr(err)

		statement, err = db.Prepare(createArchivedEpisodes)
		checkErr(err)
		_, err = statement.Exec()
		checkErr(err)

		statement, err = db.Prepare(createArchivedEpisodesPathIdx)
		checkErr(err)
		_, err = statement.Exec()
		checkErr(err)

		statement, err = db.Prepare(createSkippedEpisodes)
		checkErr(err)
		_, err = statement.Exec()
		checkErr(err)

		// Clean up historical rows with NULL or empty podcast_title
		_, err = db.Exec(`DELETE FROM episodes WHERE podcast_title IS NULL OR TRIM(podcast_title) = '';`)
		checkErr(err)
		_, err = db.Exec(`DELETE FROM interactive_episodes WHERE podcast_title IS NULL OR TRIM(podcast_title) = '';`)
		checkErr(err)
	}
}

// podEpisodesIntoDatabase adds the podcast metadata to the db.
//
// All writes for one feed are wrapped in a single transaction, and the
// per-episode statements are prepared once and reused. Previously every episode
// produced ~2 autocommitted writes (a last_seen UPDATE plus an
// interactive_episodes upsert), each forcing its own fsync — the dominant cost
// of a parse on the local ZFS pool. Batching turns that into one commit per
// feed. The SQL and the insert/update/upsert decision logic are unchanged, so
// the resulting rows are identical to the previous autocommit version; the only
// behavioural difference is that a feed's writes are now atomic (all-or-nothing
// if an error aborts mid-feed). The caller owns db and must not have it closed.
func podEpisodesIntoDatabase(db *sql.DB, pod map[string]string, episodes []M) {

	// For the podcast
	// 1. Is it in the db?
	//   If yes then update the last seen timestamp
	//   If no then add it to the db

	tx, err := db.Begin()
	checkErr(err)
	committed := false
	defer func() {
		// No-op once Commit has succeeded; otherwise unwind the feed's writes.
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// 1. Is it in the db?
	rows, err := tx.Query(`
		SELECT count(*) AS COUNT
		FROM podcasts
		WHERE podcasts.title=?
		;`, pod[title])
	checkErr(err)

	var count int
	for rows.Next() {
		err = rows.Scan(&count)
		checkErr(err)
	}

	if count > 1 {
		log.Panicln(pod[title], "is in the db more than once, this should not happen")
	}

	//   If yes then update the last seen timestamp
	if count == 1 {
		if verbose {
			log.Println(pod[title], "is already in the db")
		}

		res, err := tx.Exec(`
			UPDATE podcasts
			SET last_seen = ?
			WHERE title = ?
			;`, ts, pod[title])
		checkErr(err)

		affected, err := res.RowsAffected()
		checkErr(err)

		if verbose {
			log.Println(affected, "rows updated (last_seen)")
		}
	}

	// If no then add it to the db
	if count == 0 {
		// Check the title isn't nonsense
		checkStr(pod[title], title)

		log.Println(pod[title], "is not in the db and seems to be a new podcast, adding")

		// We wrap these because we don't want empty strings in the db ideally
		res, err := tx.Exec(`
			INSERT INTO podcasts
			(author, category, description, language, link, title, first_seen, last_seen)
			VALUES
			(?, ?, ?, ?, ?, ?, ?, ?)
			;`,
			nullWrap(pod[author]),
			nullWrap(pod[category]),
			nullWrap(pod[description]),
			nullWrap(pod[language_]),
			nullWrap(pod[link]),
			nullWrap(pod[title]),
			ts,
			ts,
		)
		checkErr(err)

		idx, err := res.LastInsertId()
		checkErr(err)
		if verbose {
			log.Println("insert id for podcast", pod[title], "is", idx)
		}
	}

	// For the episodes
	// Is it in the db?
	//   If yes then update the last seen timestamp
	//   If no then add it to the db
	//
	// Prepare the per-episode statements once and reuse them for every episode
	// in this feed/transaction, rather than re-preparing inside the loop.
	epCountStmt, err := tx.Prepare(`
		SELECT count(*) AS COUNT
		FROM episodes
		WHERE podcastname_episodename_hash=?
		;`)
	checkErr(err)
	defer epCountStmt.Close()

	// Refresh last_seen by primary key. Matching on title alone (as this used
	// to) refreshed every same-titled row across ALL podcasts — retitled
	// podcasts' leftover rows (e.g. "Aufhebunga Bunga (Patreon)" after the
	// feed became "Bungacast") were kept looking feed-fresh forever, which
	// broke any logic using last_seen to tell live rows from stale ones.
	epUpdateStmt, err := tx.Prepare(`
		UPDATE episodes
		SET last_seen = ?
		WHERE podcastname_episodename_hash = ?
		;`)
	checkErr(err)
	defer epUpdateStmt.Close()

	epInsertStmt, err := tx.Prepare(`
		INSERT INTO episodes (
			author, description, episode,
			file, format, guid,
			link, published, title,
			updated, first_seen, last_seen,
			podcast_title, podcastname_episodename_hash, file_url_hash
		) VALUES (
			?, ?, ?,
			?, ?, ?,
			?, ?, ?,
			?, ?, ?,
			?, ?, ?
		);`)
	checkErr(err)
	defer epInsertStmt.Close()

	interStmt, err := tx.Prepare(interactiveEpisodeUpsertSQL)
	checkErr(err)
	defer interStmt.Close()

	for idx := range episodes {
		// Do some type conversion map[string]interface{} to map[string]string
		ep := make(map[string]string)

		for k, v := range episodes[idx] {
			// Below is safer expansion of
			// ep[k] = v.(string)

			if v == nil {
				ep[k] = ""
				continue
			}
			strVal, ok := v.(string)
			if !ok {
				ep[k] = fmt.Sprintf("%v", v)
				continue
			}
			ep[k] = strVal
		}

		podcastNameEpisodeName := pod[title] + ep[title]
		podcastNameEpisodenameHash := fmt.Sprintf("%x", md5.Sum([]byte(podcastNameEpisodeName)))
		fileUrlHash := fmt.Sprintf("%x", md5.Sum([]byte(ep[file])))

		// Make sure description does not contain HTML
		ep[description] = strip.StripTags(ep[description])

		// Is it in the db?
		rows, err := epCountStmt.Query(podcastNameEpisodenameHash)
		checkErr(err)

		var count int
		for rows.Next() {
			err = rows.Scan(&count)
			checkErr(err)
		}

		if count > 1 {
			log.Panicln(ep[title], "is in the db more than once, this should not happen")
		}

		if count == 1 {
			if verbose {
				log.Println(ep[title], "is already in the db")
			}

			res, err := epUpdateStmt.Exec(ts, podcastNameEpisodenameHash)
			checkErr(err)

			affected, err := res.RowsAffected()
			checkErr(err)

			if verbose {
				log.Println(affected, "rows updated (last_seen)")
			}
		}

		if count == 0 {
			// Let's do some validation here so we don't put garbage in the database
			checkStr(pod[title], title)

			// From author ... updated are just the keys in the episode map
			if verbose {
				log.Println(ep)
			}

			res, err := epInsertStmt.Exec(
				nullWrap(ep[author]),
				nullWrap(ep[description]),
				nullWrap(ep[episode]),
				nullWrap(ep[file]),
				nullWrap(ep[format]),
				nullWrap(ep[guid]),
				nullWrap(ep[link]),
				nullWrap(ep[published]),
				nullWrap(ep[title]),
				nullWrap(ep[updated]),
				ts,
				ts,
				nullWrap(pod[title]),
				podcastNameEpisodenameHash,
				fileUrlHash,
			)
			checkErr(err)

			idx, err := res.LastInsertId()
			checkErr(err)
			if verbose {
				log.Println("insert id for episode", ep[title], "is", idx)
			}
		}

		err = execInteractiveUpsert(interStmt, pod[title], ep, podcastNameEpisodenameHash, fileUrlHash)
		checkErr(err)
	}

	checkErr(tx.Commit())
	committed = true
}

// interactiveEpisodeUpsertSQL upserts a row into interactive_episodes. It is
// shared by the batch parse path (prepared once per feed on a *sql.Tx) and by
// the interactive importer (prepared per call on a *sql.DB), so the two paths
// stay in lockstep.
const interactiveEpisodeUpsertSQL = `
	INSERT INTO interactive_episodes (
		author, description, episode,
		file, format, guid,
		link, published, title,
		updated, first_seen, last_seen,
		podcast_title, podcastname_episodename_hash, file_url_hash
	) VALUES (
		?, ?, ?,
		?, ?, ?,
		?, ?, ?,
		?, ?, ?,
		?, ?, ?
	)
	ON CONFLICT(podcastname_episodename_hash) DO UPDATE SET
		author = excluded.author,
		description = excluded.description,
		episode = excluded.episode,
		file = excluded.file,
		format = excluded.format,
		guid = excluded.guid,
		link = excluded.link,
		published = excluded.published,
		title = excluded.title,
		updated = excluded.updated,
		podcast_title = excluded.podcast_title,
		file_url_hash = excluded.file_url_hash,
		last_seen = excluded.last_seen
	;`

// execInteractiveUpsert runs interactiveEpisodeUpsertSQL against an
// already-prepared statement (prepared from either a *sql.DB or a *sql.Tx). The
// argument order matches the placeholders in interactiveEpisodeUpsertSQL.
func execInteractiveUpsert(stmt *sql.Stmt, podTitle string, ep map[string]string, podcastNameEpisodenameHash string, fileUrlHash string) error {
	_, err := stmt.Exec(
		nullWrap(ep[author]),
		nullWrap(ep[description]),
		nullWrap(ep[episode]),
		nullWrap(ep[file]),
		nullWrap(ep[format]),
		nullWrap(ep[guid]),
		nullWrap(ep[link]),
		nullWrap(ep[published]),
		nullWrap(ep[title]),
		nullWrap(ep[updated]),
		ts,
		ts,
		podTitle,
		podcastNameEpisodenameHash,
		fileUrlHash,
	)
	return err
}

func upsertInteractiveEpisodeRecord(db *sql.DB, podTitle string, ep map[string]string, podcastNameEpisodenameHash string, fileUrlHash string) error {
	stmt, err := db.Prepare(interactiveEpisodeUpsertSQL)
	if err != nil {
		return err
	}
	defer stmt.Close()

	return execInteractiveUpsert(stmt, podTitle, ep, podcastNameEpisodenameHash, fileUrlHash)
}

func storeParsedFeedInInteractiveTable(pod map[string]string, episodes []M) error {
	podTitle := strings.TrimSpace(pod[title])
	if podTitle == "" {
		return fmt.Errorf("podcast title is empty")
	}

	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		return err
	}
	defer db.Close()

	for idx := range episodes {
		ep := make(map[string]string)
		for key, value := range episodes[idx] {
			if value == nil {
				ep[key] = ""
				continue
			}
			strVal, ok := value.(string)
			if !ok {
				ep[key] = ""
				continue
			}
			ep[key] = strVal
		}

		podcastNameEpisodeName := podTitle + ep[title]
		podcastNameEpisodenameHash := fmt.Sprintf("%x", md5.Sum([]byte(podcastNameEpisodeName)))
		fileUrlHash := fmt.Sprintf("%x", md5.Sum([]byte(ep[file])))
		ep[description] = strip.StripTags(ep[description])

		err = upsertInteractiveEpisodeRecord(db, podTitle, ep, podcastNameEpisodenameHash, fileUrlHash)
		if err != nil {
			return err
		}
	}

	return nil
}

func recordInteractiveDownload(downloadPath string) error {
	filename := strings.TrimSpace(filepath.Base(downloadPath))
	if filename == "" {
		return fmt.Errorf("download path does not contain a filename")
	}

	hash, err := hashFromDownloadFilename(filename)
	if err != nil {
		return err
	}

	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec(`
		INSERT INTO downloads
		(filename, hash, first_seen, last_seen, tagged_at)
		VALUES
		(?, ?, ?, ?, ?)
		ON CONFLICT(filename) DO UPDATE SET
			hash = excluded.hash,
			last_seen = excluded.last_seen,
			tagged_at = excluded.tagged_at
		;`,
		filename,
		hash,
		ts,
		ts,
		ts,
	)
	return err
}

func hashFromDownloadFilename(filename string) (string, error) {
	parsedA := strings.ReplaceAll(filename, ".", "-")
	parsedB := strings.Split(parsedA, "-")
	nParsedB := len(parsedB)

	if nParsedB < 2 {
		return "", fmt.Errorf("unexpected filename format: %s", filename)
	}

	hash := strings.TrimSpace(parsedB[nParsedB-2 : nParsedB-1][0])
	if hash == "" {
		return "", fmt.Errorf("missing hash in filename: %s", filename)
	}

	return hash, nil
}

// updateDatabaseForDownloads updates the db to record the pods downloaded as downloaded
func updateDatabaseForDownloads() {
	cwd := getCwd()
	fmt.Printf("Note: updating db with downloaded files in %s\n", cwd)

	// Get files
	files := sensibleFilesInDir(cwd).ToSlice()

	// Open db
	db, err := sql.Open(sqlite3, dbFileName)
	checkErr(err)

	if err == nil {
		defer db.Close()
	}

	// Update or insert as appropriate
	for _, file := range files {
		fileStr := fmt.Sprintf("%v", file)
		hash, _, err := hashFromFilename(fileStr)
		if err != nil {
			log.Printf("skipping file %s: %v", fileStr, err)
			continue
		}

		// See if it's in already
		rows, err := db.Query(`
			SELECT count(*) AS COUNT
			FROM downloads
			WHERE filename = ?
			;`, file)
		checkErr(err)

		// If it is, update last_seen
		var count int
		for rows.Next() {
			err = rows.Scan(&count)
			checkErr(err)
		}

		if count > 1 {
			log.Panicln(file, "is in the db more than once, this should not happen")
		}

		// If yes then update the last seen timestamp
		if count == 1 {
			stmt, err := db.Prepare(`
				UPDATE downloads 
				SET hash = ?, last_seen = ?
				WHERE filename = ?
				;`)
			checkErr(err)

			res, err := stmt.Exec(hash, ts, file)
			checkErr(err)

			affected, err := res.RowsAffected()
			checkErr(err)

			if verbose {
				log.Println(affected, "rows updated (last_seen)")
			}
		}

		//   If no then add it to the db
		if count == 0 {
			log.Println(file, "is not in the db and seems to be a fresh download, adding")

			stmt, err := db.Prepare(`
				INSERT INTO downloads
				(filename, hash, first_seen, last_seen)
				VALUES
				(?, ?, ?, ?)
				;`)
			checkErr(err)

			// We wrap these because we don't want empty strings in the db ideally
			res, err := stmt.Exec(file, hash, ts, ts)
			checkErr(err)

			idx, err := res.LastInsertId()
			checkErr(err)

			if verbose {
				log.Println("insert id for podcast download", file, "is", idx)
			}
		}
	}

	// Could check to see if anything has unexpectedly disappeared but this seems pointless hence not done
}

// skippedEpisodeRecord is one download-pass retitle skip destined for the
// skipped_episodes audit table.
type skippedEpisodeRecord struct {
	episodeHash  string
	podcastTitle string
	title        string
	guid         string
	matchedHash  string
	matchedTitle string
	reason       string
}

// recordSkippedEpisodes upserts the run's retitle skips into skipped_episodes
// in one transaction: new skips get first_skipped, repeat skips refresh
// last_skipped and the match details.
func recordSkippedEpisodes(records []skippedEpisodeRecord) error {
	if len(records) == 0 {
		return nil
	}
	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO skipped_episodes
			(podcastname_episodename_hash, podcast_title, title, guid,
			 matched_episode_hash, matched_title, reason, first_skipped, last_skipped)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(podcastname_episodename_hash) DO UPDATE SET
			matched_episode_hash = excluded.matched_episode_hash,
			matched_title = excluded.matched_title,
			reason = excluded.reason,
			last_skipped = excluded.last_skipped
		;`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, r := range records {
		if _, err := stmt.Exec(r.episodeHash, r.podcastTitle, r.title, r.guid,
			r.matchedHash, r.matchedTitle, r.reason, ts, ts); err != nil {
			return err
		}
	}
	return tx.Commit()
}
