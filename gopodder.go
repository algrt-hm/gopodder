// gopodder
//
// To build:
// go build gopodder.go
////

package main

import (
	"context"
	"crypto/md5"
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
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

	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

// Set here as assume this will be fine for most cases
var db_file string = "gopodder.sqlite"

// These globals will be set in init()
var l *log.Logger // Logger
var ts string     // This will be starting timestamp
var conf_file_path_default string
var podcasts_dir_default string
var verbose bool = false // Verbosity
var cwd string           // Current working directory

// M is an alias for map[string]interface{}
type M map[string]interface{}

// Utility function to convert a string to NullString if empty
// Lifted from: https://stackoverflow.com/questions/40266633/golang-insert-null-into-sql-instead-of-empty-string
func null_wrap(s string) sql.NullString {
	if len(strings.TrimSpace(s)) == 0 {
		return sql.NullString{}
	}
	return sql.NullString{
		String: s,
		Valid:  true,
	}
}

// Utility function to check err and also give line number of the calling function
func check(e error) {
	if e != nil {
		_, _, line, _ := runtime.Caller(1)
		l.Fatalf("%s (called from: %d)", e, line)
	}
}

func print_a_few(slice_of_str []string) {
	counter := 0
	for _, str := range slice_of_str {
		fmt.Println(str)
		counter += 1
		if counter > 3 {
			fmt.Printf("And many more ...\n\n")
			break
		}
	}
}

func removeAccents(s string) string {
	t := transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)
	result, _, err := transform.String(t, s)
	check(err)
	return result
}

// Approach to remove unicode characters; this is because id2v3 < 2.4
// does not support unicode
// Lifted from: https://gist.github.com/jheth/1e74039003c52cb46a16e9eb799846a4
func CleanText(text string, maxLength int) string {
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
	text_sans_emojis := gomoji.RemoveEmojis(text)

	// Finally get rid of any accents etc
	return removeAccents(text_sans_emojis)
}

// Below two functions from https://stackoverflow.com/a/59293875
// Strange that not already in standard library, but maybe I missed it ...
// Returns true if all runes in string are upper-case letters
func IsUpper(s string) bool {
	for _, r := range s {
		if !unicode.IsUpper(r) && unicode.IsLetter(r) {
			return false
		}
	}
	return true
}

// Returns true if all runes in string are lower-case letters
func IsLower(s string) bool {
	for _, r := range s {
		if !unicode.IsLower(r) && unicode.IsLetter(r) {
			return false
		}
	}
	return true
}

// Utility function to return current working directory
func getcwd() string {
	ex, err := os.Executable()
	check(err)
	return filepath.Dir(ex)
}

// Creates our SQLite db and tables if they do not exist
func create_tables_if_not_exist() {
	create_podcasts := `
	CREATE TABLE IF NOT EXISTS podcasts (
		-- no idx needed as sqllite provides rowid
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

	create_episodes := `
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

	create_downloaded := `
	CREATE TABLE IF NOT EXISTS downloads (
		filename TEXT PRIMARY KEY,
		hash TEXT NOT NULL,
		first_seen TEXT NOT NULL,
		last_seen TEXT NOT NULL,
		tagged_at TEXT DEFAULT NULL
	);
	`

	db, err := sql.Open("sqlite3", db_file)
	check(err)

	if err == nil {
		defer db.Close()

		statement, err := db.Prepare(create_podcasts)
		check(err)
		_, err = statement.Exec()
		check(err)

		statement, err = db.Prepare(create_episodes)
		check(err)
		_, err = statement.Exec()
		check(err)

		statement, err = db.Prepare(create_episodes)
		check(err)
		_, err = statement.Exec()
		check(err)

		statement, err = db.Prepare(create_downloaded)
		check(err)
		_, err = statement.Exec()
		check(err)
	}
}

// Add the podcast metadata to the db
func pod_episodes_into_db(pod map[string]string, episodes []M) {

	// For the podcast
	// 1. Is it in the db?
	//   If yes then update the last seen timestamp
	//   If no then add it to the db

	db, err := sql.Open("sqlite3", db_file)
	check(err)
	if err == nil {
		defer db.Close()
	}

	// 1. Is it in the db?
	rows, err := db.Query(`
		SELECT count(*) AS COUNT
		FROM podcasts
		WHERE podcasts.title=?
		;`, pod["title"])
	check(err)

	var count int
	for rows.Next() {
		err = rows.Scan(&count)
		check(err)
	}

	if count > 1 {
		l.Fatalln(pod["title"], "is in the db more than once, this should not happen")
	}

	//   If yes then update the last seen timestamp
	if count == 1 {
		if verbose {
			l.Println(pod["title"], "is already in the db")
		}

		stmt, err := db.Prepare(`
			UPDATE podcasts
			SET last_seen = ?
			WHERE title = ?
			;`)
		check(err)

		res, err := stmt.Exec(ts, pod["title"])
		check(err)

		affected, err := res.RowsAffected()
		check(err)

		if verbose {
			l.Println(affected, "rows updated (last_seen)")
		}
	}

	//   If no then add it to the db
	if count == 0 {
		l.Println(pod["title"], "is not in the db and seems to be a new podcast, adding")

		stmt, err := db.Prepare(`
			INSERT INTO podcasts
			(author, category, description, language, link, title, first_seen, last_seen)
			VALUES
			(?, ?, ?, ?, ?, ?, ?, ?)
			;`)
		check(err)

		// We wrap these because we don't want empty strings in the db ideally
		res, err := stmt.Exec(
			null_wrap(pod["author"]),
			null_wrap(pod["category"]),
			null_wrap(pod["description"]),
			null_wrap(pod["language"]),
			null_wrap(pod["link"]),
			null_wrap(pod["title"]),
			ts,
			ts,
		)
		check(err)

		idx, err := res.LastInsertId()
		check(err)
		if verbose {
			l.Println("insert id for podcast", pod["title"], "is", idx)
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

		podcastname_episodename := pod["title"] + ep["title"]
		podcastname_episodename_hash := fmt.Sprintf("%x", md5.Sum([]byte(podcastname_episodename)))
		file_url_hash := fmt.Sprintf("%x", md5.Sum([]byte(ep["file"])))

		// Is it in the db?
		rows, err := db.Query(`
			SELECT count(*) AS COUNT
			FROM episodes
			WHERE podcastname_episodename_hash=?
			;`, podcastname_episodename_hash)
		check(err)

		var count int
		for rows.Next() {
			err = rows.Scan(&count)
			check(err)
		}

		if count > 1 {
			l.Fatalln(ep["title"], "is in the db more than once, this should not happen")
		}

		if count == 1 {
			if verbose {
				l.Println(ep["title"], "is already in the db")
			}

			stmt, err := db.Prepare(`
				UPDATE episodes
				SET last_seen = ?
				WHERE title = ?
				;`)
			check(err)

			res, err := stmt.Exec(ts, ep["title"])
			check(err)

			affected, err := res.RowsAffected()
			check(err)

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
			check(err)

			// Make sure descption does not contain HTML
			non_html_desc := strip.StripTags(ep["description"])
			ep["description"] = non_html_desc

			// From author ... updated are just the keys in the episode map
			if verbose {
				l.Println(ep)
			}

			res, err := stmt.Exec(
				null_wrap(ep["author"]),
				null_wrap(ep["description"]),
				null_wrap(ep["episode"]),
				null_wrap(ep["file"]),
				null_wrap(ep["format"]),
				null_wrap(ep["guid"]),
				null_wrap(ep["link"]),
				null_wrap(ep["published"]),
				null_wrap(ep["title"]),
				null_wrap(ep["updated"]),
				ts, ts,
				null_wrap(pod["title"]),
				podcastname_episodename_hash, file_url_hash,
			)
			check(err)

			idx, err := res.LastInsertId()
			check(err)
			if verbose {
				l.Println("insert id for episode", ep["title"], "is", idx)
			}
		}
	}
}

// Read configuration file and return slices with URLs for RSS files
func read_config(conf_file_path string) ([]string, error) {
	content, err := ioutil.ReadFile(conf_file_path)
	validated := []string{}

	if !errors.Is(err, os.ErrNotExist) {
		l.Println("Configuration file at " + conf_file_path + " exists")
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
		fmt.Printf("Feed URLs from config file %s:\n%s\n", conf_file_path, strings.Join(validated, "\n"))
	}
	l.Printf("%d valid URLs\n", len(validated))

	return validated, nil
}

// Parse an individual RSS feed
func parse_feed(url string) (map[string]string, []M, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Will throw the item maps in here
	var s_items []M
	pod := make(map[string]string)

	l.Println("Parsing " + url)

	fp := gofeed.NewParser()
	feed, err := fp.ParseURLWithContext(url, ctx)

	// If there is an error parsing the feed then return with the error
	if err != nil {
		return nil, nil, err
	}

	// Podcast metadata
	if len(feed.Authors) == 1 {
		pod["author"] = strings.TrimSpace(feed.Authors[0].Name)
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

	pod["title"] = strings.TrimSpace(feed.Title)
	pod["link"] = strings.TrimSpace(feed.Link)
	pod["description"] = strings.TrimSpace(feed.Description)
	pod["language"] = strings.TrimSpace(feed.Language)
	pod["category"] = strings.TrimSpace(strings.Join(feed.Categories, ", "))

	// Episodes metadata
	for idx := range feed.Items {
		item := feed.Items[idx]

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

		// We want
		//    "title", "language", "itunes:author", "feed_url", "link", "description",
		//    "itunes:summary", "itunes:explicit", "enclosure"

		i := make(M)

		i["title"] = strings.TrimSpace(item.Title)

		// Potential addition: use itunes:author if missing
		// Potential addition: Loop through for authors and handle >1
		if len(item.Authors) == 1 {
			i["author"] = strings.TrimSpace(item.Authors[0].Name)
		} else {
			if len(item.Authors) == 0 {
				// No authors
				i["author"] = ""
			} else {
				l.Println("More than one author")
				l.Println(item.Authors)
			}
		}

		i["link"] = strings.TrimSpace(item.Link)
		i["description"] = strings.TrimSpace(item.Description)
		i["guid"] = strings.TrimSpace(item.GUID)

		// Change anything time.Time into a string
		// this is buffer value
		var t time.Time

		if item.UpdatedParsed != nil {
			t = *item.UpdatedParsed
			i["updated"] = t.Format(time.RFC3339)
		} else {
			i["updated"] = ""
		}

		if item.PublishedParsed != nil {
			t = *item.PublishedParsed
			i["published"] = t.Format(time.RFC3339)
		} else {
			i["published"] = ""
		}

		// Assumes only one enclosure
		if len(item.Enclosures) == 1 {
			i["file"] = strings.TrimSpace(item.Enclosures[0].URL)
			i["format"] = strings.TrimSpace(item.Enclosures[0].Type)
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
			if i["author"] == "" {
				i["author"] = strings.TrimSpace(item.ITunesExt.Author)
			}

			// Pick up itunes episode while we are at in
			i["episode"] = strings.TrimSpace(item.ITunesExt.Episode)

			// If desc is empty use itunes summary
			if i["description"] == "" {
				i["description"] = strings.TrimSpace(item.ITunesExt.Summary)
			}
		} else {
			// Set episode to empty string if we have not picked it up
			i["episode"] = ""
		}

		s_items = append(s_items, i)
	}

	return pod, s_items, err
}

// Returns the hash part from the filename
// we can then compare this hash to what is in the db
func hash_from_filename(filename string) (string, string) {
	parsed_a := strings.ReplaceAll(filename, ".", "-")
	parsed_b := strings.Split(parsed_a, "-")
	n_parsed_b := len(parsed_b)
	hash := parsed_b[n_parsed_b-2 : n_parsed_b-1][0]
	transformed_title := parsed_b[n_parsed_b-3 : n_parsed_b-2][0]
	return hash, transformed_title
}

// Returns a set of filenames that we identify as podcasts
// Assumes .mp3 only
func sensible_files_in_dir(path string) mapset.Set {
	filenames_set := mapset.NewSet()

	files, err := os.ReadDir(path)
	check(err)

	// Put all the filenames into a set
	for _, file := range files {
		filename := file.Name()
		if filename[:2] != "._" {
			// Count the number of '-' occurances betcause if not 5 and with mp3 then not a well formed filename
			if strings.Count(filename, "-") == 5 && strings.Contains(filename, "mp3") {
				filenames_set.Add(filename)
			}
		}
	}

	return filenames_set
}

// Check the db against files we already have
func see_what_pods_we_already_have(db_file string, path string) mapset.Set {
	filenames_hash_set := mapset.NewSet()
	db_hash_set := mapset.NewSet()
	transformed_titles_set := mapset.NewSet()
	filenames_set := mapset.NewSet()
	hashes_to_ep_info := make(map[string]string)
	transformed_titles_to_hashes := make(map[string]string)
	hashes_to_transformed_titles := make(map[string]string)
	tts_in_filesname := mapset.NewSet()

	files, err := os.ReadDir(path)
	check(err)

	// Put all the filenames into a set
	for _, file := range files {
		filename := file.Name()
		if filename[:2] != "._" {
			// Count the number of '-' occurances betcause if not 5 and with mp3 then not a well formed filename
			if strings.Count(filename, "-") == 5 && strings.Contains(filename, "mp3") {
				hash, transformed_title := hash_from_filename(filename)
				filenames_hash_set.Add(hash)
				filenames_set.Add(filename)

				hashes_to_transformed_titles[hash] = transformed_title
				transformed_titles_to_hashes[transformed_title] = hash
				tts_in_filesname.Add(transformed_title)
			}
		}
	}
	filenames_slice := filenames_set.ToSlice()

	// Get all the file_url_hashes from the db too and put them into another set
	db, err := sql.Open("sqlite3", db_file)
	check(err)

	if err == nil {
		defer db.Close()
	}

	query := `SELECT podcast_title, title, file_url_hash FROM episodes WHERE file IS NOT NULL AND file !='';`

	rows, err := db.Query(query)
	check(err)

	// Iterate over the rows in the result
	for rows.Next() {
		// Data from db
		var podcast_title, title, file_url_hash, transformed_title string

		err = rows.Scan(&podcast_title, &title, &file_url_hash)
		check(err)

		// sets and maps
		transformed_title = title_transformation(title)
		transformed_titles_set.Add(transformed_title)
		transformed_titles_to_hashes[transformed_title] = file_url_hash
		hashes_to_transformed_titles[file_url_hash] = transformed_title

		db_hash_set.Add(file_url_hash)
		hashes_to_ep_info[file_url_hash] = fmt.Sprintf("%s: %s", podcast_title, title)
	}

	// slices for convenience
	db_hash_slice := db_hash_set.ToSlice()
	filesnames_hash_slice := filenames_hash_set.ToSlice()
	transformed_titles_slice := transformed_titles_set.ToSlice()

	fmt.Printf(
		"\n%d db hashes %d filename hashes %d map between the two\n",
		len(db_hash_slice),
		len(filesnames_hash_slice),
		len(hashes_to_ep_info),
	)

	// whatever is in the db that we do not have as a file
	in_db_not_in_file_set := db_hash_set.Difference(filenames_hash_set)
	in_db_not_in_file_slice := in_db_not_in_file_set.ToSlice()
	n_in_db_not_in_file := len(in_db_not_in_file_slice)

	fmt.Printf("\n%d in db and not in files based on file (URL) hashes\n\n", n_in_db_not_in_file)

	// range over the transformed titles from the db
	for _, transformed_title_from_db := range transformed_titles_slice {
		// Firstly see if it's in the hashes we are interested in
		hash_of_interest := transformed_titles_to_hashes[transformed_title_from_db.(string)]
		// if the hash is in the set of episodes in the db that we do not already have
		if in_db_not_in_file_set.Contains(hash_of_interest) {
			// l.Printf("Interested in transformed title %s", v_tts)
			// If it is then do substring search against every filename
			for _, v_fn := range filenames_slice {
				filename := v_fn.(string)
				transformed_title := transformed_title_from_db.(string)

				// Potential addition: we could also look at the new hash, but this is less imported given we also compare titles below
				if len(transformed_title) == 0 || strings.Contains(filename, transformed_title) {
					// The transformed title is in a filename and so we can remove the hash in the filename
					hash_to_remove := transformed_titles_to_hashes[transformed_title]
					in_db_not_in_file_set.Remove(hash_to_remove)
				}
			}
		}
	}

	in_db_not_in_file_slice = in_db_not_in_file_set.ToSlice()
	n_in_db_not_in_file_slice := len(in_db_not_in_file_slice)

	var fmt_str string
	if n_in_db_not_in_file_slice > 0 {
		fmt_str = "%d podcasts are in the feeds which have not been downloaded; being:\n"
	} else {
		fmt_str = "%d podcasts are in the feeds which have not been downloaded\n"
	}
	fmt.Printf(fmt_str, n_in_db_not_in_file_slice)

	// go through and remove anything with the same title
	// reason for doing this is backwards compatability with my (pre-existing) collection of pods
	// which used a different hash

	// for range in the hashes
	for _, v := range in_db_not_in_file_slice {
		// get the associated title
		tt_of_interest := hashes_to_transformed_titles[v.(string)]
		// is the tranformed title in the set of what's in the filenames?
		if tts_in_filesname.Contains(tt_of_interest) {
			// if it is, remove it
			in_db_not_in_file_set.Remove(v)
		}

	}

	in_db_not_in_file_slice = in_db_not_in_file_set.ToSlice()
	n_in_db_not_in_file_slice = len(in_db_not_in_file_slice)

	if n_in_db_not_in_file_slice > 0 {
		fmt_str = "%d podcasts are in the feeds which have not been downloaded after fn title scan; being:\n"
	} else {
		fmt_str = "%d podcasts are in the feeds which have not been downloaded after fn title scan\n"
	}
	fmt.Printf(fmt_str, n_in_db_not_in_file_slice)

	counter := 0
	for _, v := range in_db_not_in_file_slice {
		hash := v.(string)
		title := hashes_to_ep_info[hash]
		t_title := hashes_to_transformed_titles[hash]

		fmt.Printf("%s (%s) (db hash %s)\n", title, t_title, hash)

		if len([]rune(t_title)) == 0 {
			l.Fatal("No t_title")
		}

		counter += 1
		if counter > 5 {
			fmt.Printf("And many more ...\n\n")
			break
		}
	}

	// Return the pod episodes we want to download
	return in_db_not_in_file_set
}

// Takes a podcast title and transforms it (removing spaces etc)
// so it can sensibly be used in the podcast filename
func title_transformation(s string) string {
	// Get rid of any quotation characters
	var s_c string
	s_a := strings.ReplaceAll(s, "\"", "")
	s_b := strings.ReplaceAll(s_a, "'", "")

	// Make the string less than 100 chars if it is more than that
	if len(s_b) > 100 {
		s_c = s_b[:100]
	} else {
		s_c = s_b
	}

	// Get the words and join them together with _
	regex_s := `[A-Za-z]+`
	re := regexp.MustCompile(regex_s)
	str_slice := re.FindAllStringSubmatch(s_c, -1)

	new_str := make([]string, 0)

	for _, v := range str_slice {
		s := v[0]
		// If the word is all upper case make it title case
		if IsUpper(s) {
			new_str = append(new_str, strings.Title(strings.ToLower(s)))
		} else {
			new_str = append(new_str, s)
		}
	}

	return strings.Join(new_str, "_")
}

// Generate our wget shell command give a URL and filename
func gen_script_line(url string, filename string) string {
	cmd := "wget --no-clobber --continue --no-check-certificate --no-verbose"
	return fmt.Sprintf("%s '%s' -O '%s' && chmod 666 '%s'", cmd, url, filename, filename)
}

// Generates download script based on the pods to download
func generate_download_list(podcasts_dir string) {
	hashes := see_what_pods_we_already_have(db_file, podcasts_dir)

	l.Println("Pods for download are")

	// Some entries have no associated file
	// e.g. RSS feed for Risky Talk podcast includes transcripts where the file tag is empty

	// The ifnull takes first_seen if published is null
	// this sensibly handles the case where the published tag is not provided in the feed
	query := `SELECT podcast_title, IFNULL(published, first_seen), title, podcastname_episodename_hash, file_url_hash, file FROM episodes WHERE file != '' AND file IS NOT NULL;`

	db, err := sql.Open("sqlite3", db_file)
	check(err)

	if err == nil {
		defer db.Close()
	}

	rows, err := db.Query(query)
	check(err)

	filenames := make([]string, 0)
	urls := make([]string, 0)

	for rows.Next() {
		// Data from db
		var podcast_title, published, title, podcastname_episodename_hash, file_url_hash, file string
		err = rows.Scan(&podcast_title, &published, &title, &podcastname_episodename_hash, &file_url_hash, &file)
		check(err)

		// If file_url_hash in hashes ...
		if hashes.Contains(file_url_hash) {
			transformed_title := title_transformation(title)
			transformed_podcast_title := title_transformation(podcast_title)
			short_date_a := []rune(published)
			short_date := string(short_date_a[:10])

			new_filename := fmt.Sprintf("%s-%s-%s-%s.mp3", transformed_podcast_title, short_date, transformed_title, podcastname_episodename_hash)

			filenames = append(filenames, new_filename)
			urls = append(urls, file)
		}
	}

	// some output to keep user informed
	print_a_few(filenames)

	// for anything that's in filenames but not in download_hopper_filenames we should
	// put in the script

	// slice of strings for the script lines
	lines := make([]string, 0)
	for i := range filenames {
		lines = append(lines, gen_script_line(urls[i], filenames[i]))
	}

	if len(lines) == 0 {
		fmt.Println("Nothing to add to the download script ...")
		// We don't return here because we want the script to be empty
		// if there is nothing to download
	} else {
		fmt.Println("Lines being added to script are ...")
		print_a_few(lines)
	}

	// then we scan for the files and add those that went into the script to the download_hopper table
	// if they are in fact in the folder i.e. downloaded

	filename := podcasts_dir + "/download_pods.sh"
	lines_joined := strings.Join(lines, "\n")
	// Potential addition: permissions should probably be narrower
	err = ioutil.WriteFile(filename, []byte(lines_joined), 0666)
	check(err)

	l.Printf("Written script to %s", filename)
}

func run_eyed3(filename string) {
	fmt.Println("Reading with eyeD3:")
	cmd := exec.Command("/usr/local/bin/python3.9", "/usr/local/bin/eyeD3", filename)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	check(err)

	fmt.Println("Removing tags with eyeD3:")
	cmd = exec.Command("/usr/local/bin/python3.9", "/usr/local/bin/eyeD3", "--remove-all", filename)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	check(err)
}

// Run the download script with the wget commands to download the pods
func run_download_script(podcasts_dir string) {
	cwd := getcwd()
	if cwd != podcasts_dir {
		fmt.Printf("Note: current working dir is %s, which is where the podcasts will be saved into\n", getcwd())
		fmt.Printf("we assume the pod downloading script is in %s however\n", podcasts_dir)
	}

	cmd := exec.Command("/bin/sh", podcasts_dir+"/download_pods.sh")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	check(err)
}

// Updates the db to record the pods downloaded as downloaded
func update_db_for_downloads() {
	cwd := getcwd()
	fmt.Printf("Note: updating db with downloaded files in %s\n", cwd)

	// Get files
	files := sensible_files_in_dir(cwd).ToSlice()

	// Open db
	db, err := sql.Open("sqlite3", db_file)
	check(err)
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
		check(err)

		// If it is, update last_seen
		var count int
		for rows.Next() {
			err = rows.Scan(&count)
			check(err)
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
			check(err)

			res, err := stmt.Exec(ts, file)
			check(err)

			affected, err := res.RowsAffected()
			check(err)

			if verbose {
				l.Println(affected, "rows updated (last_seen)")
			}
		}

		//   If no then add it to the db
		if count == 0 {
			l.Println(file, "is not in the db and seems to be a fresh download, adding")

			// Get the hash from the filename
			hash, _ := hash_from_filename(file.(string))

			stmt, err := db.Prepare(`
				INSERT INTO downloads
				(filename, hash, first_seen, last_seen)
				VALUES
				(?, ?, ?, ?)
				;`)
			check(err)

			// We wrap these because we don't want empty strings in the db ideally
			res, err := stmt.Exec(file, hash, ts, ts)
			check(err)

			idx, err := res.LastInsertId()
			check(err)

			if verbose {
				l.Println("insert id for podcast download", file, "is", idx)
			}
		}
	}

	// Could check to see if anything has unexpectedly disappeared but this seems pointless hence not done
}

// Tags the file at filename with the title and album metadata
func tag_single_pod(filename string, title string, album string) {
	// title tag is title
	// album is podcast_title
	// genre is "Podcast"

	genre := "Podcast"
	var parse bool = true

	tag, err := id3v2.Open(filename, id3v2.Options{Parse: parse})

	if err != nil {
		fmt.Printf("%s is a file where reading the tags has been problematic %s\n", filename, err)
		// fmt.Printf("You can try blitzing the tag data with: eyeD3 --remove-all %s", filename)
		run_eyed3(filename)

		// e.g.
		// eyeD3 --remove-all Entitled_Opinions_about_Life_and_Literature-2021-12-03-The_Uses_of_Trauma_with_Alex_Rex-96de32981a4c34a2ca933854613183ee.mp3

		parse = false
		tag, err = id3v2.Open(filename, id3v2.Options{Parse: parse})
		check(err)
	} else {
		defer tag.Close()
	}

	// If we get to here then we're writing
	ascii_title := CleanText(title, len(title))
	ascii_album := CleanText(album, len(album))
	ascii_genre := CleanText(genre, len(genre))

	// Remove all existing frames
	// particularly anything in comment and lyrics as there can be garbage in there (again Risky Talk podcast looking at you lol)

	// tag.DeleteAllFrames()

	if parse {
		// If parsed then set only if not equal to what we would set them to
		if tag.Title() != ascii_title {
			tag.SetTitle(ascii_title)
		}
		if tag.Album() != ascii_album {
			tag.SetAlbum(ascii_album)
		}
		if tag.Genre() != ascii_genre {
			tag.SetGenre(ascii_genre)
		}
	} else {
		tag.SetTitle(ascii_title)
		tag.SetAlbum(ascii_album)
		tag.SetGenre(ascii_genre)
	}

	// Want to mentioned that we tagged the files!
	comment := id3v2.CommentFrame{
		Encoding:    tag.DefaultEncoding(),
		Language:    "eng",
		Description: "Tagged by",
		Text:        "gopodder",
	}
	tag.AddCommentFrame(comment)

	// Write tags
	err = tag.Save()

	if err != nil {
		fmt.Println("Title:", ascii_title, "Album:", ascii_album, "Genre:", ascii_genre)
	}
	check(err)
}

// Tag all the podcasts
func tag_those_pods(podcasts_dir string) int {
	fmt.Printf("Note: will tag pods in current working directory %s\n", getcwd())
	// fmt.Println("If the count is 0 make sure you ran -u after downloading")

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
	db, err := sql.Open("sqlite3", db_file)
	check(err)
	if err == nil {
		defer db.Close()
	}

	rows, err := db.Query(query)
	check(err)

	// If it is, update last_seen
	var count int = 0
	for rows.Next() {
		var ns_filename, ns_podcast_title, ns_title, ns_description sql.NullString
		err = rows.Scan(&ns_filename, &ns_podcast_title, &ns_title, &ns_description)
		check(err)

		filename := ns_filename.String
		podcast_title := ns_podcast_title.String
		title := ns_title.String
		l.Printf("%s: %s / %s", filename, podcast_title, title)

		// Tag 'em
		tag_single_pod(filename, title, podcast_title)
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
			check(err)

			res, err := stmt.Exec(ts, ind_filename)
			check(err)

			affected, err := res.RowsAffected()
			check(err)

			if affected != 1 {
				l.Fatal("More than one row affected which should not happen")
			}
		}

		// Done message
		if podcasts_dir != cwd {
			fmt.Printf("Now we are done with tagging you can move your podcasts from the current directory to your podcast directory with \nmv ./*mp3 %s\n", podcasts_dir)
		}
	}

	return count
}

// Parse each of the feeds in the config file
func parse_em(conf_file_path string) {
	urls, err := read_config(conf_file_path + "/gopodder.conf")
	check(err)

	for _, url := range urls {
		podcast, episodes, err := parse_feed(url)
		check(err)

		pod_episodes_into_db(podcast, episodes)
	}
}

// init() function is called automatically before main() in Go
// we use this to set up the logger and some globals
func init() {
	// logger
	l = log.New(os.Stdout, os.Args[0]+" ", log.LstdFlags|log.Lshortfile)

	// this is our timestamp
	ts = time.Now().Format(time.RFC3339)

	// defaults for conf file and podcast dir
	cwd = getcwd()
	conf_file_path_default = cwd
	podcasts_dir_default = cwd
}

func cwd_check(cwd string) {
	if cwd == "/home/mike/repos/gopodder" || cwd == "/usr/home/mike/repos/gopodder" {
		fmt.Println("Quiting as cwd=", cwd)
		os.Exit(1)
	}
}

func main() {
	// Get conf file path from env
	conf_var_envname := "GOPODCONF"
	conf_file_path, conf_var_isset := os.LookupEnv(conf_var_envname)

	path_var_envname := "GOPODDIR"
	podcasts_dir, path_var_isset := os.LookupEnv(path_var_envname)

	// Use default if not set
	// we let the user know a bit further down
	if !conf_var_isset {
		conf_file_path = conf_file_path_default
	}

	if !path_var_isset {
		podcasts_dir = podcasts_dir_default
	}

	// argparse biz

	helpstring := `
Incrementally download and tag podcasts. Requires wget

Typical use:
	-p to parse
	-s to write script
	-d to download into current working dir (%s)
	-u to update db for downloads
	-t to tag

	-a will do each of the above in order

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
	parser := argparse.NewParser(os.Args[0], fmt.Sprintf(helpstring, cwd, conf_file_path, cwd, podcasts_dir))

	// Create flag(s)
	parse_opt_ptr := parser.Flag("p", "parse", &argparse.Options{Required: false, Help: "Parse podcast feeds"})
	see_opt_ptr := parser.Flag("s", "see", &argparse.Options{Required: false, Help: "See what pods we already have and write script"})
	download_pods := parser.Flag("d", "download", &argparse.Options{Required: false, Help: "Download pods from script written with -s/--see"})
	post_dl_update := parser.Flag("u", "update", &argparse.Options{Required: false, Help: "Update db for what we have downloaded"})
	tag_pods := parser.Flag("t", "tag", &argparse.Options{Required: false, Help: "Tag freshly downloaded pods"})
	do_all := parser.Flag("a", "all", &argparse.Options{Required: false, Help: "Same as -psdut"})
	verbose_opt := parser.Flag("v", "verbose", &argparse.Options{Required: false, Help: "Verbose"})
	experimental_opt := parser.Flag("e", "experimental", &argparse.Options{Required: false, Help: "WIP"})

	// Parser for shell args
	err := parser.Parse(os.Args)

	// Set global if verbose flag set
	if *verbose_opt {
		verbose = true
	}

	// In case of error print error and print usage
	// This can also be done by passing -h or --help flags
	if err != nil {
		fmt.Print(parser.Usage(err))
	}

	if parse_opt_ptr == nil || see_opt_ptr == nil {
		l.Fatal("Some issue with argparse")
	}

	// Given we know we have gone past the help message now let's
	// warn people if we are using defaults

	tmp_fmt := "%s is not set; using default, value is %s\n"
	if !conf_var_isset {
		l.Printf(tmp_fmt, conf_var_envname, conf_file_path)
	}

	if !path_var_isset {
		l.Printf(tmp_fmt, path_var_envname, podcasts_dir)
	}

	cwd := getcwd()
	if *do_all || *download_pods {
		// If we are downloading we make sure we are not downloading into home or similar
		cwd_check(cwd)
	}
	// First let's get the tables ready to go and create them if not
	create_tables_if_not_exist()

	if *do_all {
		parse_em(conf_file_path)
		generate_download_list(podcasts_dir)
		run_download_script(podcasts_dir)
		update_db_for_downloads()
		tag_those_pods(podcasts_dir)
	} else {
		if *parse_opt_ptr {
			parse_em(conf_file_path)
		}

		if *see_opt_ptr {
			generate_download_list(podcasts_dir)
		}

		if *download_pods {
			run_download_script(podcasts_dir)
		}

		if *post_dl_update {
			update_db_for_downloads()
		}

		if *tag_pods {
			tag_those_pods(podcasts_dir)
		}

		if *experimental_opt {
			fmt.Println("Nothing to see here")
		}

	}
}
