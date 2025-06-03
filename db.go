package main

import (
	"crypto/md5"
	"database/sql"
	"fmt"
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

		statement, err = db.Prepare(createEpisodes)
		checkErr(err)
		_, err = statement.Exec()
		checkErr(err)

		statement, err = db.Prepare(createDownloaded)
		checkErr(err)
		_, err = statement.Exec()
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
		log.Fatalln(pod[title], "is in the db more than once, this should not happen")
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

	//   If no then add it to the db
	if count == 0 {
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
			ep[k] = v.(string)
		}

		podcastNameEpisodeName := pod[title] + ep[title]
		podcastNameEpisodenameHash := fmt.Sprintf("%x", md5.Sum([]byte(podcastNameEpisodeName)))
		fileUrlHash := fmt.Sprintf("%x", md5.Sum([]byte(ep[file])))

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
			log.Fatalln(ep[title], "is in the db more than once, this should not happen")
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

			// Make sure description does not contain HTML
			nonHtmlDesc := strip.StripTags(ep[description])
			ep[description] = nonHtmlDesc

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
	}
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
			log.Fatalln(file, "is in the db more than once, this should not happen")
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
			hash, _ := hashFromFilename(file.(string))

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
