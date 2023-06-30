////
// gopodder
//
// To build:
// go build gopodder.go
////

package main

import (
	"context"
	"crypto/md5"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"
	"unicode"

	"github.com/akamensky/argparse"
	"github.com/bogem/id3v2/v2"
	mapset "github.com/deckarep/golang-set"
	"github.com/forPelevin/gomoji"
	strip "github.com/grokify/html-strip-tags-go"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mmcdole/gofeed"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

// Convenience constants
const gopodder = "gopodder"
const confFile = gopodder + ".conf"
const dbFileName = gopodder + ".sqlite"
const confVarEnvName = "GOPODCONF"
const pathVarEnvName = "GOPODDIR"
const sqlite3 = "sqlite3" // Used with sql.Open
const author = "author"
const category = "category"
const description = "description"
const language_ = "language"
const link = "link"
const title = "title"
const episode = "episode"
const file = "file"
const format = "format"
const guid = "guid"
const published = "published"
const updated = "updated"
const mp3 = "mp3"
const eyeD3 = "eyeD3"

// These globals will be set in init()
var l *log.Logger // Logger
var ts string     // This will be starting timestamp
var confFilePathDefault string
var podcastsDirDefault string
var verbose bool = false // Verbosity. TODO: use this more often to reduce the output a bit
var cwd string           // Current working directory

// TODO: Should refactor so these are not globals
var pythonPath string
var eyeD3Path string

// M is an alias for map[string]interface{}
type M map[string]interface{}

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

// checkErr is a utility function to checkErr err and also give line number of the calling function
func checkErr(err error) {
	if err != nil {
		_, _, line, _ := runtime.Caller(1)
		l.Fatalf("%s (called from: %d)", err, line)
	}
}

// isExecAny returns true if any of the execute bits are set
// Source: https://stackoverflow.com/a/60128480
func isExecAny(mode os.FileMode) bool {
	return mode&0111 != 0
}

// readFirstLine reads the first line of a script
// and returns the interpreter (shebang) path
func readFirstLine(path string) string {
	data, err := os.ReadFile(path)
	checkErr(err)
	// sheBang is the first line of the script (no jokes please)
	// https://en.wikipedia.org/wiki/Shebang_(Unix)
	sheBang := strings.Split(string(data), "\n")[0]
	return strings.Replace(sheBang, "#!", "", 1)
}

// getExecutableFileNames returns a slice of strings of executable files in a directory
func getExecutableFileNames(dir string) ([]string, error) {
	var files []string

	fd, err := os.Open(dir)
	if err != nil {
		return files, err
	}

	fileInfo, err := fd.Readdir(-1)
	fd.Close()
	if err != nil {
		return files, err
	}

	for _, file := range fileInfo {
		if isExecAny(file.Mode()) {
			files = append(files, file.Name())
		}
	}

	return files, nil
}

// checkDependencies checks if wget and eyeD3 are in PATH
// and returns the python interpreter path associated with eyeD3, the path of eyeD3
func checkDependencies(verbose bool) (bool, string, string) {
	// check path is set
	path := os.Getenv("PATH")
	// set to false, meaning missing, by default
	haveWget := false
	haveEyeD3 := false
	// set to empty string by default
	pythonInterpreter := ""
	eyeD3Dir := ""

	if path == "" {
		l.Fatal("PATH does not seem to be set in environment")
	}

	pathDirs := strings.Split(path, ":")
	nPathDirs := len(pathDirs)

	for idx, dir := range pathDirs {
		if verbose {
			l.Printf("Checking PATH dir %d/%d: %s", idx+1, nPathDirs, dir)
		}
		filesInFolder, err := getExecutableFileNames(dir)

		if err != nil {
			if verbose {
				l.Printf("Error reading files in %s: %s", dir, err)
			}
			continue
		}

		for _, fileIn := range filesInFolder {
			if fileIn == "wget" {
				haveWget = true
			}
			if fileIn == eyeD3 {
				haveEyeD3 = true
				eyeD3Dir = dir
			}
			if haveWget && haveEyeD3 {
				break
			}
		}
	}

	if haveEyeD3 {
		l.Printf("%s found in %s", eyeD3, eyeD3Dir)
		eyeD3Path := eyeD3Dir + "/" + eyeD3
		pythonInterpreter = readFirstLine(eyeD3Path)
	}

	if haveWget && haveEyeD3 {
		l.Printf("Dependencies look good: have wget and %s", eyeD3)
		return true, pythonInterpreter, eyeD3Dir
	}

	l.Printf("PATH contains %d folders: %s", nPathDirs, path)
	if !haveWget {
		l.Println("FAIL: no wget")
		return false, pythonInterpreter, eyeD3Dir
	}

	if !haveEyeD3 {
		l.Printf("FAIL: no %s", eyeD3)
		return false, pythonInterpreter, eyeD3Dir
	}

	l.Println("FAIL: unknown reason")
	return false, pythonInterpreter, eyeD3Dir
}

// printSome is a utility function to print a few items from a slice
func printSome(sliceOfStr []string) {
	counter := 0
	for _, str := range sliceOfStr {
		fmt.Println(str)
		counter += 1
		if counter > 3 {
			fmt.Printf("And many more ...\n\n")
			break
		}
	}
}

// removeAccents removes accents from a string
// Lifted from: https://stackoverflow.com/a/65981868
func removeAccents(s string) string {
	t := transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)
	result, _, err := transform.String(t, s)
	checkErr(err)
	return result
}

// cleanText removes/sorts unicode characters; this is because id2v3 < 2.4
// does not support unicode
// Lifted from: https://gist.github.com/jheth/1e74039003c52cb46a16e9eb799846a4
func cleanText(text string, maxLength int) string {
	if len(text) < 5 {
		return ""
	}

	if strings.Contains(text, "\n") {
		sections := strings.Split(text, "\n")
		newText := sections[0]
		for idx, s := range sections {
			// Append sections until we reach the max length
			if idx > 0 && len(newText) < maxLength {
				newText = newText + " " + s
			}
		}
		text = newText
	}

	var charMap = map[string]string{
		"′":          "'",
		"|":          "",
		"\u20ac":     "e",   // euro
		"\u0026":     "and", // ampersand
		"\u1ebd":     "e",
		"\u200b":     " ",
		"\u200e":     " ",
		"\u2010":     "-",
		"\u2013":     "-",
		"\u2014":     "-",
		"\u2018":     "'",
		"\u2019":     "'",
		"\u2022":     "-",
		"\u2026":     "...",
		"\u2028":     "",
		"\u2033":     "\"",
		"\u2034":     "\"",
		"\u2035":     "'",
		"\u2036":     "\"",
		"\u2037":     "\"",
		"\u2038":     ".",
		"\u2044":     "/",
		"\u201a":     ",",
		"\u201b":     "'",
		"\u201c":     "\"",
		"\u201d":     "\"",
		"\u201e":     "\"",
		"\u201f":     "\"",
		"\u2122":     "",
		"\u2600":     "",
		"\u263a":     "",
		"\u26fa":     "",
		"\u27a2":     ">",
		"\ufe0f":     "",
		"\xa0":       " ",
		"\xa2":       "",
		"\xae":       "",
		"\xbd":       "",
		"\xde":       "",
		"\xe2":       "",
		"\xe9":       "",
		"\xfc":       "u",
		"\U0001f44c": "",
		"\U0001f44d": "",
		"\U0001f642": "",
		"\U0001f601": "",
		"\U0001f690": "",
		"\U0001f334": "",
		"\U0001f3dd": "",
		"\U0001f3fd": "",
		"\U0001f3d6": "",
		"\U0001f3a3": "",
		"\U0001f525": "", // flame
		"\U0001f60a": "", // smiley
	}

	// Scan the string replacing all characters in the map
	newText := ""
	for _, c := range text {
		newC, ok := charMap[string(c)]
		// If not found, use the original
		if !ok {
			newC = string(c)
		}
		newText = newText + newC
	}
	text = newText

	if len(text) > maxLength {
		return text[0:maxLength-3] + "..."
	}

	// Remove any emojis (this is my own addition to the gist mentioned at the top in the comment)
	// and then get rid of any accents finally
	return removeAccents(gomoji.RemoveEmojis(text))
}

// isUpper is a utility function to check if a string is all upper-case
// Below two functions from https://stackoverflow.com/a/59293875
// Strange that not already in standard library, but maybe I missed it ...
// Returns true if all runes in string are upper-case letters
func isUpper(s string) bool {
	for _, r := range s {
		if !unicode.IsUpper(r) && unicode.IsLetter(r) {
			return false
		}
	}
	return true
}

// getCwd is a utility function to return current working directory
func getCwd() string {
	ex, err := os.Executable()
	checkErr(err)
	return filepath.Dir(ex)
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
		l.Fatalln(pod[title], "is in the db more than once, this should not happen")
	}

	//   If yes then update the last seen timestamp
	if count == 1 {
		if verbose {
			l.Println(pod[title], "is already in the db")
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
			l.Println(affected, "rows updated (last_seen)")
		}
	}

	//   If no then add it to the db
	if count == 0 {
		l.Println(pod[title], "is not in the db and seems to be a new podcast, adding")

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
			l.Println("insert id for podcast", pod[title], "is", idx)
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
			l.Fatalln(ep[title], "is in the db more than once, this should not happen")
		}

		if count == 1 {
			if verbose {
				l.Println(ep[title], "is already in the db")
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
				l.Println(affected, "rows updated (last_seen)")
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
				l.Println(ep)
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
				l.Println("insert id for episode", ep[title], "is", idx)
			}
		}
	}
}

// readConfig is a function which reads the configuration file and return slices with URLs for RSS files
func readConfig(confFilePath string) ([]string, error) {
	content, err := ioutil.ReadFile(confFilePath)
	validated := []string{}

	if !errors.Is(err, os.ErrNotExist) {
		l.Println("Configuration file at " + confFilePath + " exists")
	}

	if err != nil {
		return nil, err
	}

	s := strings.Split(string(content), "\n")

	// Want to look through the slices and only keep those that have http in them
	for i := range s {
		if strings.Contains(s[i], "http") {
			validated = append(validated, s[i])
		}
	}

	if verbose {
		fmt.Printf("Feed URLs from config file %s:\n%s\n", confFilePath, strings.Join(validated, "\n"))
	}
	l.Printf("%d valid URLs\n", len(validated))

	return validated, nil
}

// parseFeed a function to to parse an individual RSS feed
func parseFeed(url string) (map[string]string, []M, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Will throw the item maps in here
	var sItems []M
	pod := make(map[string]string)

	l.Println("Parsing " + url)
	fp := gofeed.NewParser()

	fp.Client = &http.Client{
		// Extend the timeout a bit. See also: https://github.com/mmcdole/gofeed/issues/83#issuecomment-355485788
		Timeout: 20 * time.Second,
		// Allow various ciphers. See also: https://github.com/golang/go/issues/44267#issuecomment-819278575
		Transport: &http.Transport{
			TLSHandshakeTimeout: 10 * time.Second,
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS12,
				CipherSuites: []uint16{
					tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
					tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
					tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305, // Go 1.8 only
					tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,   // Go 1.8 only
					tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
					tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				},
			},
		}}

	feed, err := fp.ParseURLWithContext(url, ctx)

	// If there is an error parsing the feed then return with the error
	if err != nil {
		return nil, nil, err
	}

	// Podcast metadata
	if len(feed.Authors) == 1 {
		pod[author] = strings.TrimSpace(feed.Authors[0].Name)
	} else {
		if len(feed.Authors) == 0 {
			// No authors
			if verbose {
				l.Println("No authors")
			}
		} else {
			l.Println("More than one author")
			l.Println(feed.Authors)
		}
	}

	pod[title] = strings.TrimSpace(feed.Title)
	pod[link] = strings.TrimSpace(feed.Link)
	pod[description] = strings.TrimSpace(feed.Description)
	pod[language_] = strings.TrimSpace(feed.Language)
	pod[category] = strings.TrimSpace(strings.Join(feed.Categories, ", "))

	// Episodes metadata
	for idx := range feed.Items {
		item := feed.Items[idx]

		// Looks like:
		//
		// type Item struct {
		//     Title           string                   `json:"title,omitempty"`
		//     Description     string                   `json:"description,omitempty"`
		//     Content         string                   `json:"content,omitempty"`
		//     Link            string                   `json:"link,omitempty"`
		//     Links           []string                 `json:"links,omitempty"`
		//     Updated         string                   `json:"updated,omitempty"`
		//     UpdatedParsed   *time.Time               `json:"updatedParsed,omitempty"`
		//     Published       string                   `json:"published,omitempty"`
		//     PublishedParsed *time.Time               `json:"publishedParsed,omitempty"`
		//     Author          *Person                  `json:"author,omitempty"` // Deprecated
		//     Authors         []*Person                `json:"authors,omitempty"`
		//     GUID            string                   `json:"guid,omitempty"`
		//     Image           *Image                   `json:"image,omitempty"`
		//     Categories      []string                 `json:"categories,omitempty"`
		//     Enclosures      []*Enclosure             `json:"enclosures,omitempty"`
		//     DublinCoreExt   *ext.DublinCoreExtension `json:"dcExt,omitempty"`
		//     ITunesExt       *ext.ITunesItemExtension `json:"itunesExt,omitempty"`
		//     Extensions      ext.Extensions           `json:"extensions,omitempty"`
		//     Custom          map[string]string        `json:"custom,omitempty"`
		// }

		// We want:
		//
		//    "title", "language", "itunes:author", "feed_url", "link", "description",
		//    "itunes:summary", "itunes:explicit", "enclosure"

		i := make(M)

		i[title] = strings.TrimSpace(item.Title)

		// Potential addition: use itunes:author if missing
		// Potential addition: Loop through for authors and handle >1
		if len(item.Authors) == 1 {
			i[author] = strings.TrimSpace(item.Authors[0].Name)
		} else {
			if len(item.Authors) == 0 {
				// No authors
				i[author] = ""
			} else {
				l.Println("More than one author")
				l.Println(item.Authors)
			}
		}

		i[link] = strings.TrimSpace(item.Link)
		i[description] = strings.TrimSpace(item.Description)
		i[guid] = strings.TrimSpace(item.GUID)

		// Change anything time.Time into a string
		// this is buffer value
		var t time.Time

		if item.UpdatedParsed != nil {
			t = *item.UpdatedParsed
			i[updated] = t.Format(time.RFC3339)
		} else {
			i[updated] = ""
		}

		if item.PublishedParsed != nil {
			t = *item.PublishedParsed
			i[published] = t.Format(time.RFC3339)
		} else {
			i[published] = ""
		}

		// Assumes only one enclosure
		if len(item.Enclosures) == 1 {
			i[file] = strings.TrimSpace(item.Enclosures[0].URL)
			i[format] = strings.TrimSpace(item.Enclosures[0].Type)
		} else {
			if len(item.Enclosures) == 0 {
				// Enclosures is empty
			} else {
				// If it's not empty and more than one then log
				l.Println("More than one enclosure")
				l.Println(item.Enclosures)
			}
		}

		// iTunes extension to spec (not always present)
		if item.ITunesExt != nil {
			// If author is an empty string (i.e. false) then use the iTunes author
			if i[author] == "" {
				i[author] = strings.TrimSpace(item.ITunesExt.Author)
			}

			// Pick up itunes episode while we are at in
			i[episode] = strings.TrimSpace(item.ITunesExt.Episode)

			// If desc is empty use itunes summary
			if i[description] == "" {
				i[description] = strings.TrimSpace(item.ITunesExt.Summary)
			}
		} else {
			// Set episode to empty string if we have not picked it up
			i[episode] = ""
		}

		sItems = append(sItems, i)
	}

	return pod, sItems, err
}

// hashFromFilename returns the hash part from the filename
// we can then compare this hash to what is in the db
func hashFromFilename(filename string) (string, string) {
	parsedA := strings.ReplaceAll(filename, ".", "-")
	parsedB := strings.Split(parsedA, "-")
	nParsedB := len(parsedB)
	hash := parsedB[nParsedB-2 : nParsedB-1][0]
	transformedTitle := parsedB[nParsedB-3 : nParsedB-2][0]
	return hash, transformedTitle
}

// sensibleFilesInDir returns a set of filenames that we identify as podcasts
// Assumes .mp3 only
func sensibleFilesInDir(path string) mapset.Set {
	filenamesSet := mapset.NewSet()

	files, err := os.ReadDir(path)
	checkErr(err)

	// Put all the filenames into a set
	for _, file := range files {
		filename := file.Name()
		if filename[:2] != "._" {
			// Count the number of '-' occurrences because if not (5 and filename contains mp3) then not a well formed filename
			if strings.Count(filename, "-") == 5 && strings.Contains(filename, mp3) {
				filenamesSet.Add(filename)
			}
		}
	}

	return filenamesSet
}

// seeWhatPodsWeAlreadyHave will check the db against files we already have
func seeWhatPodsWeAlreadyHave(dbFile string, path string) mapset.Set {
	filenamesHashSet := mapset.NewSet()
	dbHashSet := mapset.NewSet()
	transformedTitlesSet := mapset.NewSet()
	filenamesSet := mapset.NewSet()
	hashesToEpInfo := make(map[string]string)
	transformedTitlesToHashes := make(map[string]string)
	hashesToTransformedTitles := make(map[string]string)
	ttsInFileNames := mapset.NewSet()

	files, err := os.ReadDir(path)
	checkErr(err)

	// Put all the filenames into a set
	for _, file := range files {
		filename := file.Name()
		if filename[:2] != "._" {
			// Count the number of '-' occurrences because if not 5 and with mp3 then not a well formed filename
			if strings.Count(filename, "-") == 5 && strings.Contains(filename, mp3) {
				hash, transformedTitle := hashFromFilename(filename)
				filenamesHashSet.Add(hash)
				filenamesSet.Add(filename)

				hashesToTransformedTitles[hash] = transformedTitle
				transformedTitlesToHashes[transformedTitle] = hash
				ttsInFileNames.Add(transformedTitle)
			}
		}
	}
	filenamesSlice := filenamesSet.ToSlice()

	// Get all the file_url_hashes from the db too and put them into another set
	db, err := sql.Open(sqlite3, dbFile)
	checkErr(err)

	if err == nil {
		defer db.Close()
	}

	query := `SELECT podcast_title, title, file_url_hash FROM episodes WHERE file IS NOT NULL AND file !='';`
	rows, err := db.Query(query)
	checkErr(err)

	// Iterate over the rows in the result
	for rows.Next() {
		// Data from db
		var podcastTitle, title, fileUrlHash, transformedTitle string

		err = rows.Scan(&podcastTitle, &title, &fileUrlHash)
		checkErr(err)

		// sets and maps
		transformedTitle = titleTransformation(title)
		transformedTitlesSet.Add(transformedTitle)
		transformedTitlesToHashes[transformedTitle] = fileUrlHash
		hashesToTransformedTitles[fileUrlHash] = transformedTitle

		dbHashSet.Add(fileUrlHash)
		hashesToEpInfo[fileUrlHash] = fmt.Sprintf("%s: %s", podcastTitle, title)
	}

	// slices for convenience
	dbHashSlice := dbHashSet.ToSlice()
	fileNamesHashSlice := filenamesHashSet.ToSlice()
	transformedTitlesSlice := transformedTitlesSet.ToSlice()

	fmt.Printf(
		"\n%d db hashes %d filename hashes %d map between the two\n",
		len(dbHashSlice),
		len(fileNamesHashSlice),
		len(hashesToEpInfo),
	)

	// whatever is in the db that we do not have as a file
	inDbNotInFileSet := dbHashSet.Difference(filenamesHashSet)
	inDbNotInFileSlice := inDbNotInFileSet.ToSlice()
	nInDbNotInFile := len(inDbNotInFileSlice)

	fmt.Printf("\n%d in db and not in files based on file (URL) hashes\n\n", nInDbNotInFile)

	// range over the transformed titles from the db
	for _, transformedTitleFromDb := range transformedTitlesSlice {
		// Firstly see if it's in the hashes we are interested in
		hashOfInterest := transformedTitlesToHashes[transformedTitleFromDb.(string)]
		// if the hash is in the set of episodes in the db that we do not already have
		if inDbNotInFileSet.Contains(hashOfInterest) {
			// l.Printf("Interested in transformed title %s", v_tts)
			// If it is then do substring search against every filename
			for _, v_fn := range filenamesSlice {
				filename := v_fn.(string)
				transformedTitle := transformedTitleFromDb.(string)

				// Potential addition: we could also look at the new hash, but this is less imported given we also compare titles below
				if len(transformedTitle) == 0 || strings.Contains(filename, transformedTitle) {
					// The transformed title is in a filename and so we can remove the hash in the filename
					hashToRemove := transformedTitlesToHashes[transformedTitle]
					inDbNotInFileSet.Remove(hashToRemove)
				}
			}
		}
	}

	inDbNotInFileSlice = inDbNotInFileSet.ToSlice()
	nInDbNotInFileSlice := len(inDbNotInFileSlice)

	var fmt_str string
	if nInDbNotInFileSlice > 0 {
		fmt_str = "%d podcasts are in the feeds which have not been downloaded; being:\n"
	} else {
		fmt_str = "%d podcasts are in the feeds which have not been downloaded\n"
	}
	fmt.Printf(fmt_str, nInDbNotInFileSlice)

	// go through and remove anything with the same title
	// reason for doing this is backwards compatibility with my (pre-existing) collection of pods
	// which used a different hash

	// for range in the hashes
	for _, v := range inDbNotInFileSlice {
		// get the associated title
		ttOfInterest := hashesToTransformedTitles[v.(string)]
		// is the transformed title in the set of what's in the filenames?
		if ttsInFileNames.Contains(ttOfInterest) {
			// if it is, remove it
			inDbNotInFileSet.Remove(v)
		}
	}

	inDbNotInFileSlice = inDbNotInFileSet.ToSlice()
	nInDbNotInFileSlice = len(inDbNotInFileSlice)

	if nInDbNotInFileSlice > 0 {
		fmt_str = "%d podcasts are in the feeds which have not been downloaded after fn title scan; being:\n"
	} else {
		fmt_str = "%d podcasts are in the feeds which have not been downloaded after fn title scan\n"
	}
	fmt.Printf(fmt_str, nInDbNotInFileSlice)

	counter := 0
	for _, v := range inDbNotInFileSlice {
		hash := v.(string)
		title := hashesToEpInfo[hash]
		tTitle := hashesToTransformedTitles[hash]

		fmt.Printf("%s (%s) (db hash %s)\n", title, tTitle, hash)

		if len([]rune(tTitle)) == 0 {
			l.Fatal("No tTitle")
		}

		counter += 1
		if counter > 5 {
			fmt.Printf("And many more ...\n\n")
			break
		}
	}

	// Return the pod episodes we want to download
	return inDbNotInFileSet
}

// titleTransformation takes a podcast title and transforms it (removing spaces etc)
// so it can sensibly be used in the podcast filename
func titleTransformation(s string) string {

	// Reflect strings.Title functionality; strings.Title is deprecated
	caser := cases.Title(language.English)

	// Get rid of any quotation characters
	var sC string
	sA := strings.ReplaceAll(s, "\"", "")
	sB := strings.ReplaceAll(sA, "'", "")

	// Make the string less than 100 runes if it is more than that
	if len(sB) > 100 {
		sC = sB[:100]
	} else {
		sC = sB
	}

	// Get the words and join them together with _
	regexS := `[A-Za-z]+`
	re := regexp.MustCompile(regexS)
	strSlice := re.FindAllStringSubmatch(sC, -1)

	newStr := make([]string, 0)

	for _, v := range strSlice {
		s := v[0]
		// If the word is all upper case make it title case
		if isUpper(s) {
			buff := caser.String(strings.ToLower(s))
			newStr = append(newStr, buff)
		} else {
			newStr = append(newStr, s)
		}
	}

	return strings.Join(newStr, "_")
}

// genScriptLine generate our wget shell command give a URL and filename
func genScriptLine(url string, filename string) string {
	return fmt.Sprintf("wget --no-clobber --continue --no-check-certificate --no-verbose '%s' -O '%s' && chmod 666 '%s'", url, filename, filename)
}

// generateDownloadList generates download script based on the pods to download
func generateDownloadList(podcastsDir string) {
	hashes := seeWhatPodsWeAlreadyHave(dbFileName, podcastsDir)

	l.Println("Pods for download are")

	// Some entries have no associated file
	// e.g. RSS feed for Risky Talk podcast includes transcripts where the file tag is empty

	// The ifnull takes first_seen if published is null
	// this sensibly handles the case where the published tag is not provided in the feed
	query := `SELECT podcast_title, IFNULL(published, first_seen), title, podcastname_episodename_hash, file_url_hash, file FROM episodes WHERE file != '' AND file IS NOT NULL;`

	db, err := sql.Open(sqlite3, dbFileName)
	checkErr(err)

	if err == nil {
		defer db.Close()
	}

	rows, err := db.Query(query)
	checkErr(err)

	filenames := make([]string, 0)
	urls := make([]string, 0)

	for rows.Next() {
		// Data from db
		var podcastTitle, published, title, podcastNameEpisodenameHash, fileUrlHash, file string
		err = rows.Scan(&podcastTitle, &published, &title, &podcastNameEpisodenameHash, &fileUrlHash, &file)
		checkErr(err)

		// If file_url_hash in hashes ...
		if hashes.Contains(fileUrlHash) {
			transformedTitle := titleTransformation(title)
			transformedPodcastTitle := titleTransformation(podcastTitle)
			shortDateA := []rune(published)
			shortDate := string(shortDateA[:10])

			newFilename := fmt.Sprintf("%s-%s-%s-%s.%s", transformedPodcastTitle, shortDate, transformedTitle, podcastNameEpisodenameHash, mp3)
			filenames = append(filenames, newFilename)
			urls = append(urls, file)
		}
	}

	// some output to keep user informed
	printSome(filenames)

	// for anything that's in filenames but not in download_hopper_filenames we should
	// put in the script

	// slice of strings for the script lines
	lines := make([]string, 0)
	for i := range filenames {
		lines = append(lines, genScriptLine(urls[i], filenames[i]))
	}

	if len(lines) == 0 {
		fmt.Println("Nothing to add to the download script ...")
		// We don't return here because we want the script to be empty
		// if there is nothing to download
	} else {
		fmt.Println("Lines being added to script are ...")
		printSome(lines)
	}

	// Then we scan for the files and add those that went into the script to the download_hopper table
	// if they are in fact in the folder i.e. downloaded
	filename := podcastsDir + "/download_pods.sh"
	linesJoined := strings.Join(lines, "\n")

	// Potential addition: permissions should probably be narrower
	err = ioutil.WriteFile(filename, []byte(linesJoined), 0666)
	checkErr(err)

	l.Printf("Written script to %s", filename)
}

// runEyeD3 runs the eyeD3 command to strip tags from the mp3 file
func runEyeD3(filename string, pythonPath string, eyeD3Path string) {
	t := eyeD3Path + "/" + eyeD3
	eyeD3Path = t

	fmt.Printf("Using: %s %s %s\n", pythonPath, eyeD3Path, filename)

	fmt.Printf("Reading with %s:", eyeD3)
	cmd := exec.Command(pythonPath, eyeD3Path, filename)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	checkErr(err)

	fmt.Printf("Removing tags with %s:", eyeD3)
	cmd = exec.Command(pythonPath, eyeD3Path, "--remove-all", filename)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	checkErr(err)
}

// runDownloadScript run the download script with the wget commands to download the pods
func runDownloadScript(podcasts_dir string) {
	cwd := getCwd()
	if cwd != podcasts_dir {
		fmt.Printf("Note: current working dir is %s, which is where the podcasts will be saved into\n", getCwd())
		fmt.Printf("we assume the pod downloading script is in %s however\n", podcasts_dir)
	}

	cmd := exec.Command("/bin/sh", podcasts_dir+"/download_pods.sh")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	checkErr(err)
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
			l.Fatalln(file, "is in the db more than once, this should not happen")
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
				l.Println(affected, "rows updated (last_seen)")
			}
		}

		//   If no then add it to the db
		if count == 0 {
			l.Println(file, "is not in the db and seems to be a fresh download, adding")

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
				l.Println("insert id for podcast download", file, "is", idx)
			}
		}
	}

	// Could check to see if anything has unexpectedly disappeared but this seems pointless hence not done
}

// tagSinglePod tags the file at filename with the title and album metadata
func tagSinglePod(filename string, title string, album string) {
	// title tag is title
	// album is podcast title

	// genre is "Podcast"
	const genre = "Podcast"
	var parse bool = true

	tag, err := id3v2.Open(filename, id3v2.Options{Parse: parse})

	if err != nil {
		fmt.Printf("%s is a file where reading the tags has been problematic %s\n", filename, err)
		runEyeD3(filename, pythonPath, eyeD3Path)
		parse = false
		tag, err = id3v2.Open(filename, id3v2.Options{Parse: parse})
		checkErr(err)
	} else {
		defer tag.Close()
	}

	// If we get to here then we're writing
	asciiTitle := cleanText(title, len(title))
	asciiAlbum := cleanText(album, len(album))
	asciiGenre := cleanText(genre, len(genre))

	if parse {
		// If parsed then set only if not equal to what we would set them to
		if tag.Title() != asciiTitle {
			tag.SetTitle(asciiTitle)
		}
		if tag.Album() != asciiAlbum {
			tag.SetAlbum(asciiAlbum)
		}
		if tag.Genre() != asciiGenre {
			tag.SetGenre(asciiGenre)
		}
	} else {
		tag.SetTitle(asciiTitle)
		tag.SetAlbum(asciiAlbum)
		tag.SetGenre(asciiGenre)
	}

	// Want to mentioned that we tagged the files!
	comment := id3v2.CommentFrame{
		Encoding:    tag.DefaultEncoding(),
		Language:    "eng",
		Description: "Tagged by",
		Text:        gopodder,
	}
	tag.AddCommentFrame(comment)

	// Write tags
	err = tag.Save()

	if err != nil {
		fmt.Println("Title:", asciiTitle, "Album:", asciiAlbum, "Genre:", asciiGenre)
	}
	checkErr(err)
}

// tagThosePods tag all the podcasts
func tagThosePods(podcasts_dir string) int {
	fmt.Printf("Note: will tag pods in current working directory %s\n", getCwd())

	filenames_set := mapset.NewSet()

	// Get the metadata for those files in the download table
	query := `
	SELECT
		filename,
		COALESCE(podcast_title, 'title missing') AS podcast_title,
		COALESCE(title, 'title missing') AS title,
		COALESCE(description, 'description missing') AS description
	FROM downloads AS d
	JOIN episodes AS e ON d.hash = e.podcastname_episodename_hash
	WHERE tagged_at IS null
	ORDER BY filename
	;
	`
	// Open db
	db, err := sql.Open(sqlite3, dbFileName)
	checkErr(err)
	if err == nil {
		defer db.Close()
	}

	rows, err := db.Query(query)
	checkErr(err)

	// If it is, update last_seen
	// count tracks the number of rows / loop iterations
	var count int = 0
	for rows.Next() {
		var ns_filename, ns_podcast_title, ns_title, ns_description sql.NullString
		err = rows.Scan(&ns_filename, &ns_podcast_title, &ns_title, &ns_description)
		checkErr(err)

		filename := ns_filename.String
		podcast_title := ns_podcast_title.String
		title := ns_title.String
		l.Printf("%s: %s / %s", filename, podcast_title, title)

		// Tag 'em
		tagSinglePod(filename, title, podcast_title)
		count += 1
		// Set of filenames we need to update in the db
		filenames_set.Add(ns_filename)
	}

	l.Printf("Been through tagging on %d files", count)

	if count == 0 {
		fmt.Println("Did you run -u after downloading with -d ?")
	} else {
		// Timestamp the tagged_at column in the download table
		for _, ind_filename := range filenames_set.ToSlice() {
			stmt, err := db.Prepare(`
				UPDATE downloads SET tagged_at = ?
				WHERE filename = ?
				;`)
			checkErr(err)

			res, err := stmt.Exec(ts, ind_filename)
			checkErr(err)

			affected, err := res.RowsAffected()
			checkErr(err)

			if affected != 1 {
				l.Fatal("More than one row affected which should not happen")
			}
		}

		// Done message
		if podcasts_dir != cwd {
			fmt.Printf("Now we are done with tagging you can move your podcasts from the current directory to your podcast directory with \nmv ./*%s %s\n", mp3, podcasts_dir)
		}
	}

	return count
}

// parseThem parses each of the feeds in the config file
func parseThem(conf_file_path string) {
	urls, err := readConfig(conf_file_path + "/" + confFile)
	checkErr(err)

	for _, url := range urls {
		podcast, episodes, err := parseFeed(url)
		checkErr(err)

		podEpisodesIntoDatabase(podcast, episodes)
	}
}

// init function is called automatically before main() in Go
// we use this to set up the logger and some globals
func init() {
	// logger
	l = log.New(os.Stdout, os.Args[0]+" ", log.LstdFlags|log.Lshortfile)

	// this is our timestamp
	ts = time.Now().Format(time.RFC3339)

	// defaults for conf file and podcast dir
	cwd = getCwd()
	confFilePathDefault = cwd
	podcastsDirDefault = cwd

}

// cwdCheck stops me from running this in my gopodder repo
// TODO: this doesn't generalise
func cwdCheck(cwd string) {
	if cwd == "/home/mike/repos/gopodder" || cwd == "/usr/home/mike/repos/gopodder" {
		fmt.Println("Exiting as cwd is", cwd)
		os.Exit(1)
	}
}

type latestPodResult struct {
	author        sql.NullString
	title         sql.NullString
	published     sql.NullString
	podcast_title sql.NullString
}

func nullStrToStr(s sql.NullString) string {
	if s.Valid {
		return s.String
	} else {
		return "?"
	}
}

func latestPodsFromDb(path string) {
	// TODO: other places should also take into account the full path to the db
	// rather than assuming it will be in cwd/same place as the binary

	newDbFileName := path + "/" + dbFileName

	db, err := sql.Open(sqlite3, newDbFileName)
	checkErr(err)

	if err == nil {
		fmt.Printf("Connected to db %s\n", newDbFileName)
		defer db.Close()
	}

	query := `select 
	  author, 
	  title, 
	  published,
	  podcast_title 
	from episodes 
	order by published desc 
	limit 100;`

	rows, err := db.Query(query)
	checkErr(err)

	var count int
	var latest latestPodResult
	var tsStr string

	for rows.Next() {
		err = rows.Scan(
			&latest.author,
			&latest.title,
			&latest.published,
			&latest.podcast_title,
		)
		checkErr(err)
		count += 1

		if latest.published.Valid {
			tt, err := time.Parse(time.RFC3339, latest.published.String)
			checkErr(err)
			tsStr = tt.Format("2 January 2006 15:04 MST")
		} else {
			tsStr = "?"
		}

		fmt.Printf("%s / %s / %s / %s\n",
			tsStr,
			nullStrToStr(latest.author),
			nullStrToStr(latest.podcast_title),
			nullStrToStr(latest.title),
		)
	}
}

func main() {
	// Get conf file path from env
	confFilePath, confVarIsSet := os.LookupEnv(confVarEnvName)
	podcastsDir, pathVarIsSet := os.LookupEnv(pathVarEnvName)

	// Use default if not set
	// we let the user know a bit further down
	if !confVarIsSet {
		confFilePath = confFilePathDefault
	}

	if !pathVarIsSet {
		podcastsDir = podcastsDirDefault
	}

	// Parse command line arguments

	// This is our help message with %s placeholders
	helpMessage := `
Incrementally download and tag podcasts. Requires wget and eyeD3

Typical use:
	-p to parse
	-s to write script
	-d to download into current working dir (%s)
	-u to update db for downloads
	-t to tag

	-a will do each of the above in order

Utility:
	-l will list the (up to) 100 latest podcasts from the db

Note:
	Will look in %s for configuration file (set $GOPODCONF to change);
	will save pods into %s; and
	you then mv into %s (set $GOPODDIR to change) (if different folder) once done
	`
	// Suggest -h for usage info if no args
	if len(os.Args) == 1 {
		fmt.Println(os.Args[0] + " -h\nfor usage info")
	}

	// Create new parser object
	parser := argparse.NewParser(os.Args[0], fmt.Sprintf(helpMessage, cwd, confFilePath, cwd, podcastsDir))

	// Create flag(s)
	parseOptPtr := parser.Flag("p", "parse", &argparse.Options{Required: false, Help: "Parse podcast feeds"})
	seeOptPtr := parser.Flag("s", "see", &argparse.Options{Required: false, Help: "See what pods we already have and write script"})
	downloadPods := parser.Flag("d", "download", &argparse.Options{Required: false, Help: "Download pods from script written with -s/--see"})
	postDlUpdate := parser.Flag("u", "update", &argparse.Options{Required: false, Help: "Update db for what we have downloaded"})
	tagPods := parser.Flag("t", "tag", &argparse.Options{Required: false, Help: "Tag freshly downloaded pods"})
	doAll := parser.Flag("a", "all", &argparse.Options{Required: false, Help: "Same as -psdut"})
	verboseOpt := parser.Flag("v", "verbose", &argparse.Options{Required: false, Help: "Verbose"})
	listLatestPods := parser.Flag("l", "list", &argparse.Options{Required: false, Help: "List latest pods"})

	// Parser for shell args
	err := parser.Parse(os.Args)

	// Set global if verbose flag set
	if *verboseOpt {
		verbose = true
	}

	// In case of error print error and print usage
	// this can also be done by passing -h or --help flags
	if err != nil {
		fmt.Print(parser.Usage(err))
	}

	if parseOptPtr == nil || seeOptPtr == nil {
		l.Fatal("Some issue with argparse")
	}

	// Given we know we have gone past the help message now let's
	// warn people if we are using defaults
	tmp_fmt := "%s is not set; using default, value is %s\n"
	if !confVarIsSet {
		l.Printf(tmp_fmt, confVarEnvName, confFilePath)
	}

	if !pathVarIsSet {
		l.Printf(tmp_fmt, pathVarEnvName, podcastsDir)
	}

	// Check we have dependencies and get python path
	haveDependancies, python, eyeD3Dir := checkDependencies(verbose)

	// Set globals
	// TODO: globals should be refactored out
	pythonPath = python
	eyeD3Path = eyeD3Dir

	l.Printf("Have dependencies: %t", haveDependancies)
	l.Printf("python path is %s", pythonPath)
	l.Printf("eyeD3 folder is %s", eyeD3Path)

	// If we don't have dependencies then we exit
	if !haveDependancies {
		l.Fatal("Exiting as we do not have dependancies")
	}

	cwd := getCwd()
	if *doAll || *downloadPods {
		// If we are downloading we make sure we are not downloading into home or similar
		cwdCheck(cwd)
	}

	// First let's get the tables ready to go and create them if not
	createTablesIfNotExist()

	if *doAll {
		parseThem(confFilePath)
		generateDownloadList(podcastsDir)
		runDownloadScript(podcastsDir)
		updateDatabaseForDownloads()
		tagThosePods(podcastsDir)
	} else {
		if *parseOptPtr {
			parseThem(confFilePath)
		}

		if *seeOptPtr {
			generateDownloadList(podcastsDir)
		}

		if *downloadPods {
			runDownloadScript(podcastsDir)
		}

		if *postDlUpdate {
			updateDatabaseForDownloads()
		}

		if *tagPods {
			tagThosePods(podcastsDir)
		}

		if *listLatestPods {
			latestPodsFromDb(confFilePath)
		}
	}
}
