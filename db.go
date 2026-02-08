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
	author        sql.NullString
	title         sql.NullString
	published     sql.NullString
	podcast_title sql.NullString
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

		statement, err = db.Prepare(createInteractiveEpisodesFileUrlHashIdx)
		checkErr(err)
		_, err = statement.Exec()
		checkErr(err)

		statement, err = db.Prepare(createDownloaded)
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

// podEpisodesIntoDatabase adds the podcast metadata to the db
func podEpisodesIntoDatabase(pod map[string]string, episodes []M) {

	// For the podcast
	// 1. Is it in the db?
	//   If yes then update the last seen timestamp
	//   If no then add it to the db

	db, err := sql.Open(sqlite3, dbFileName)
	checkErr(err)
	if err == nil {
		defer db.Close()
	}

	// 1. Is it in the db?
	rows, err := db.Query(`
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

		stmt, err := db.Prepare(`
			UPDATE podcasts
			SET last_seen = ?
			WHERE title = ?
			;`)
		checkErr(err)

		res, err := stmt.Exec(ts, pod[title])
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

		stmt, err := db.Prepare(`
			INSERT INTO podcasts
			(author, category, description, language, link, title, first_seen, last_seen)
			VALUES
			(?, ?, ?, ?, ?, ?, ?, ?)
			;`)
		checkErr(err)

		// We wrap these because we don't want empty strings in the db ideally
		res, err := stmt.Exec(
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
		rows, err := db.Query(`
			SELECT count(*) AS COUNT
			FROM episodes
			WHERE podcastname_episodename_hash=?
			;`, podcastNameEpisodenameHash)
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

			stmt, err := db.Prepare(`
				UPDATE episodes
				SET last_seen = ?
				WHERE title = ?
				;`)
			checkErr(err)

			res, err := stmt.Exec(ts, ep[title])
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

			stmt, err := db.Prepare(`
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

			// From author ... updated are just the keys in the episode map
			if verbose {
				log.Println(ep)
			}

			res, err := stmt.Exec(
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

		err = upsertInteractiveEpisodeRecord(db, pod[title], ep, podcastNameEpisodenameHash, fileUrlHash)
		checkErr(err)
	}
}

func upsertInteractiveEpisodeRecord(db *sql.DB, podTitle string, ep map[string]string, podcastNameEpisodenameHash string, fileUrlHash string) error {
	stmt, err := db.Prepare(`
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
		;`)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(
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
				SET last_seen = ?
				WHERE filename = ?
				;`)
			checkErr(err)

			res, err := stmt.Exec(ts, file)
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

			// Get the hash from the filename
			fileStr := fmt.Sprintf("%v", file)
			hash, _, err := hashFromFilename(fileStr)
			// Skip over error
			if err != nil {
				log.Printf("skipping file %s: %v", fileStr, err)
				continue
			}

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
