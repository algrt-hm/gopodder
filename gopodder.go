////
// gopodder
//
// To build:
// go build gopodder.go
////

package main

import (
	"database/sql"
	"errors"
	"fmt"
	logger "log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/akamensky/argparse"         // akin to Python argparse
	"github.com/bogem/id3v2/v2"             // id3v2 library
	mapset "github.com/deckarep/golang-set" // allows easy set functionality
	_ "github.com/mattn/go-sqlite3"         // sqlite3 driver that conforms to the built-in database/sql interface
)

// Convenience constants
const gopodder = "gopodder"
const confFile = gopodder + ".conf"
const dbFileName = gopodder + ".sqlite"
const confVarEnvName = "GOPODCONF"
const pathVarEnvName = "GOPODDIR"
const archivesVarEnvName = "GOPODDIR_ARCHIVES"
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

// feedParseWorkers bounds how many RSS feeds parseThem fetches concurrently.
// Feed fetching is network-bound and is by far the dominant cost of a run, so
// this is the main lever on total runtime. Database writes remain fully
// serialized regardless of this value: a single consumer drains the results.
const feedParseWorkers = 10

// These globals will be set in init()
// Log
var log *logger.Logger

// This will be starting timestamp
var ts string

// These will be set to cwd if not explicitly set
var confFilePathDefault string
var podcastsDirDefault string

// Verbosity. TODO: this should use the logger and loglevels
var verbose bool = false

// Current working directory so we don't have to call it more than once
var cwd string

// M is an alias for map[string]interface{} for convenience
type M map[string]interface{}

// readConfig is a function which reads the configuration file and return slices with URLs for RSS files
func readConfig(confFilePath string) ([]string, error) {
	/*
		Our config file is expected to literally just be a list of URLs of RSS files, separated by newlines e.g:

		https://example.com/podcasts/all_pods.rss
		https://poddist.com/789759379342749/podcasts.rss
	*/

	content, err := os.ReadFile(confFilePath)
	validated := []string{}

	if !errors.Is(err, os.ErrNotExist) {
		log.Println("Configuration file at " + confFilePath + " exists")
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
	log.Printf("%d valid URLs\n", len(validated))

	return validated, nil
}

// hashFromFilename returns the hash and transformed title parts from the filename.
// Example filename: "My_Podcast-2024-01-02-Episode_Title-abc123def.mp3"
func hashFromFilename(filename string) (string, string, error) {
	parsedA := strings.ReplaceAll(filename, ".", "-")
	parsedB := strings.Split(parsedA, "-")
	nParsedB := len(parsedB)
	if nParsedB < 3 {
		return "", "", fmt.Errorf("unexpected filename format (need at least 3 dash-separated parts): %s", filename)
	}
	hash := parsedB[nParsedB-2 : nParsedB-1][0]
	transformedTitle := parsedB[nParsedB-3 : nParsedB-2][0]
	return hash, transformedTitle, nil
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

// scanLocalPodFiles scans the filesystem for podcast files across one or more
// directories, returning unioned sets/maps of their hashes and titles. The
// first path is treated as the primary podcasts dir; any extras are typically
// archive scan paths from $GOPODDIR_ARCHIVES. If any path cannot be read, this
// panics via checkErr — abort loud rather than silently regenerate the
// download script.
func scanLocalPodFiles(paths []string) (hashSet, filenamesSet, ttsInFileNames mapset.Set, hashesToTT, ttToHashes map[string]string) {
	hashSet = mapset.NewSet()
	filenamesSet = mapset.NewSet()
	ttsInFileNames = mapset.NewSet()
	hashesToTT = make(map[string]string)
	ttToHashes = make(map[string]string)

	for _, path := range paths {
		files, err := os.ReadDir(path)
		checkErr(err)

		for _, file := range files {
			filename := file.Name()
			if len(filename) < 2 || filename[:2] == "._" {
				continue
			}
			if strings.Count(filename, "-") == 5 && strings.Contains(filename, mp3) {
				hash, transformedTitle, err := hashFromFilename(filename)
				if err != nil {
					log.Printf("skipping file %s: %v", filename, err)
					continue
				}
				hashSet.Add(hash)
				filenamesSet.Add(filename)
				hashesToTT[hash] = transformedTitle
				ttToHashes[transformedTitle] = hash
				ttsInFileNames.Add(transformedTitle)
			}
		}
	}
	return
}

// fetchDbEpisodeHashes queries the database for episode hashes and returns sets/maps for comparison.
func fetchDbEpisodeHashes(dbFile string) (dbHashSet, transformedTitlesSet mapset.Set, hashesToEpInfo, ttToHashes, hashesToTT map[string]string) {
	dbHashSet = mapset.NewSet()
	transformedTitlesSet = mapset.NewSet()
	hashesToEpInfo = make(map[string]string)
	ttToHashes = make(map[string]string)
	hashesToTT = make(map[string]string)

	db, err := sql.Open(sqlite3, dbFile)
	checkErr(err)

	if err == nil {
		defer db.Close()
	}

	query := `SELECT podcast_title, title, podcastname_episodename_hash FROM episodes WHERE file IS NOT NULL AND file !='';`
	rows, err := db.Query(query)
	checkErr(err)

	for rows.Next() {
		var podcastTitle, title, episodeHash string
		err = rows.Scan(&podcastTitle, &title, &episodeHash)
		checkErr(err)

		transformedTitle := titleTransformation(title)
		transformedTitlesSet.Add(transformedTitle)
		ttToHashes[transformedTitle] = episodeHash
		hashesToTT[episodeHash] = transformedTitle

		dbHashSet.Add(episodeHash)
		hashesToEpInfo[episodeHash] = fmt.Sprintf("%s: %s", podcastTitle, title)
	}
	return
}

// matchByTransformedTitle removes hashes from candidates where the transformed title
// matches an existing filename via substring search.
func matchByTransformedTitle(candidates mapset.Set, ttToHashes map[string]string, filenamesSlice []interface{}, ttsInFileNames mapset.Set) {
	candidateSlice := candidates.ToSlice()

	// Substring match: if a DB title appears in any local filename, remove its hash
	for _, v := range candidateSlice {
		hashStr := fmt.Sprintf("%v", v)
		ttOfInterest := ttToHashes[hashStr]
		if ttOfInterest == "" {
			continue
		}
		for _, v_fn := range filenamesSlice {
			filename := fmt.Sprintf("%v", v_fn)
			if len(ttOfInterest) == 0 || strings.Contains(filename, ttOfInterest) {
				candidates.Remove(v)
				break
			}
		}
	}

	// Backwards compatibility pass: remove by exact transformed-title match
	candidateSlice = candidates.ToSlice()
	for _, v := range candidateSlice {
		hashStr := fmt.Sprintf("%v", v)
		ttOfInterest := ttToHashes[hashStr]
		if ttsInFileNames.Contains(ttOfInterest) {
			candidates.Remove(v)
		}
	}
}

// seeWhatPodsWeAlreadyHave will check the db against files we already have.
// scanPaths is the list of directories to scan: typically [podcastsDir] plus
// any extras from $GOPODDIR_ARCHIVES. Hashes registered in archived_episodes
// (see Option B / --register-archive) are also treated as "already have".
func seeWhatPodsWeAlreadyHave(dbFile string, scanPaths []string) mapset.Set {
	// Scan local files (across primary + archive scan paths)
	fileHashSet, filenamesSet, ttsInFileNames, localHashesToTT, localTTToHashes := scanLocalPodFiles(scanPaths)
	filenamesSlice := filenamesSet.ToSlice()

	// DB-backed archive registry: hashes here count as "already have" even if
	// the corresponding file isn't visible on any current scan path.
	archivedHashSet := fetchArchivedHashes()

	// Fetch DB episode hashes
	dbHashSet, _, hashesToEpInfo, dbTTToHashes, dbHashesToTT := fetchDbEpisodeHashes(dbFile)

	// Merge maps: DB entries take priority for tt<->hash mappings used in matching
	for tt, hash := range localTTToHashes {
		if _, exists := dbTTToHashes[tt]; !exists {
			dbTTToHashes[tt] = hash
		}
	}
	for hash, tt := range localHashesToTT {
		if _, exists := dbHashesToTT[hash]; !exists {
			dbHashesToTT[hash] = tt
		}
	}

	fmt.Printf(
		"\n%d db hashes %d filename hashes %d archived hashes %d map between the two\n",
		dbHashSet.Cardinality(),
		fileHashSet.Cardinality(),
		archivedHashSet.Cardinality(),
		len(hashesToEpInfo),
	)

	// Whatever is in the db that we do not have on disk and is not registered as archived
	haveHashSet := fileHashSet.Union(archivedHashSet)
	inDbNotInFileSet := dbHashSet.Difference(haveHashSet)
	fmt.Printf("\n%d in db and not on disk or registered as archived\n\n", inDbNotInFileSet.Cardinality())

	// Backwards-compatibility fallback: only use title matching if there are
	// local new-scheme files but none of them matched a db hash.
	if fileHashSet.Cardinality() > 0 && inDbNotInFileSet.Cardinality() == dbHashSet.Cardinality() {
		matchByTransformedTitle(inDbNotInFileSet, dbHashesToTT, filenamesSlice, ttsInFileNames)
	}

	inDbNotInFileSlice := inDbNotInFileSet.ToSlice()
	nInDbNotInFileSlice := len(inDbNotInFileSlice)

	if nInDbNotInFileSlice > 0 {
		fmt.Printf("%d podcasts are in the feeds which have not been downloaded after fn title scan; being:\n", nInDbNotInFileSlice)
	} else {
		fmt.Printf("%d podcasts are in the feeds which have not been downloaded after fn title scan\n", nInDbNotInFileSlice)
	}

	counter := 0
	for _, v := range inDbNotInFileSlice {
		hash := fmt.Sprintf("%v", v)
		title := hashesToEpInfo[hash]
		tTitle := dbHashesToTT[hash]

		fmt.Printf("%s (%s) (db hash %s)\n", title, tTitle, hash)

		if len([]rune(tTitle)) == 0 {
			log.Panic("No tTitle")
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

// generateDownloadList generates download script based on the pods to download.
// podcastsDir is the primary podcasts directory (and where download_pods.sh is
// written). scanPaths is the full set of directories to scan when deciding
// what's already downloaded — typically [podcastsDir, ...archives].
func generateDownloadList(podcastsDir string, scanPaths []string) {
	hashes := seeWhatPodsWeAlreadyHave(dbFileName, scanPaths)

	log.Println("Pods for download are")

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
		if hashes.Contains(podcastNameEpisodenameHash) {
			newFilename := buildNonInteractiveFilename(podcastTitle, title, published, podcastNameEpisodenameHash)
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
	err = os.WriteFile(filename, []byte(linesJoined), 0666)
	checkErr(err)

	log.Printf("Written script to %s", filename)
}

func buildNonInteractiveFilename(podcastTitle, episodeTitle, publishedOrFirstSeen, podcastHash string) string {
	shortDateA := []rune(publishedOrFirstSeen)
	shortDate := string(shortDateA[:10])
	return buildEpisodeFilenameWithHash(podcastTitle, episodeTitle, shortDate, podcastHash)
}

// stripTagsWithEyeD3 runs the eyeD3 command to strip tags from the mp3 file
func stripTagsWithEyeD3(filename string, pythonInterpreterPath string, eyeD3Path string) {
	// string with path to 'binary'
	t := eyeD3Path + "/" + eyeD3
	eyeD3Path = t

	fmt.Printf("Using: %s %s %s\n", pythonInterpreterPath, eyeD3Path, filename)

	fmt.Printf("Reading with %s:", eyeD3)
	cmd := exec.Command(pythonInterpreterPath, eyeD3Path, filename)
	cmd.Stdout = os.Stdout
	// eyeD3 writes informational messages (e.g. "No ID3 v1.x/v2.x tag found!")
	// to stderr; route them to stdout so they're captured when stdout is redirected
	cmd.Stderr = os.Stdout
	err := cmd.Run()
	checkErr(err)

	fmt.Printf("Removing tags with %s:", eyeD3)
	cmd = exec.Command(pythonInterpreterPath, eyeD3Path, "--remove-all", filename)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stdout
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
	// wget writes its progress output to stderr; route it to stdout so it's
	// captured when stdout is redirected
	cmd.Stderr = os.Stdout
	err := cmd.Run()
	checkErr(err)
}

// tagSinglePod tags the file at filename with the title and album metadata
func tagSinglePod(filename string, title string, album string, pythonPath string, eyeD3Dir string) {

	// title tag is title
	// album is podcast title
	// genre is "Podcast"
	const genre = "Podcast"
	var parse bool = true

	tag, err := id3v2.Open(filename, id3v2.Options{Parse: parse})

	// if we fail to open we run eyeD3 to remove any tags and try again
	if err != nil {
		fmt.Printf("%s is a file where reading the tags has been problematic %s\n", filename, err)
		stripTagsWithEyeD3(filename, pythonPath, eyeD3Dir)
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
func tagThosePods(podcasts_dir string, pythonPath string, eyeD3Dir string) int {
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
		log.Printf("%s: %s / %s", filename, podcast_title, title)

		// Tag 'em
		tagSinglePod(filename, title, podcast_title, pythonPath, eyeD3Dir)
		count += 1
		// Set of filenames we need to update in the db
		filenames_set.Add(ns_filename)
	}

	log.Printf("Been through tagging on %d files", count)

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
				log.Panic("More than one row affected which should not happen")
			}
		}

		// Done message
		if podcasts_dir != cwd {
			fmt.Printf("Now we are done with tagging you can move your podcasts from the current directory to your podcast directory with \nmv ./*%s %s\n", mp3, podcasts_dir)
		}
	}

	return count
}

// parseThem parses each of the feeds in the config file.
//
// Feeds are fetched concurrently by a bounded pool of workers because the work
// is network-bound and each URL is independent. Crucially, the results are
// consumed by this single goroutine, so every database write still happens one
// at a time, exactly as in the previous sequential version — SQLite is never
// touched by more than one goroutine. The error handling is also preserved:
// checkErr panics (aborting the run) on a non-http error and logs-and-skips on
// an http error, just as before. The only observable differences are that the
// "Parsing ..." log lines may now interleave and a fatal error may surface
// after some other feeds have already been written; the final DB state is
// identical because every row is keyed by hash/title and every timestamp uses
// the single global ts, so write order does not matter.
func parseThem(conf_file_path string) {
	urls, err := readConfig(conf_file_path + "/" + confFile)
	checkErr(err)

	type feedResult struct {
		podcast  map[string]string
		episodes []M
		err      error
	}

	jobs := make(chan string)
	results := make(chan feedResult)

	// Never spawn more workers than there are feeds.
	workers := feedParseWorkers
	if len(urls) < workers {
		workers = len(urls)
	}

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for url := range jobs {
				podcast, episodes, parseErr := parseFeed(url)
				results <- feedResult{podcast, episodes, parseErr}
			}
		}()
	}

	// Hand out the work, then close results once every feed has been fetched.
	go func() {
		for _, url := range urls {
			jobs <- url
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	// One shared DB handle for the whole parse. The consumer below is the only
	// writer, so cap the pool at a single connection (no SQLite lock contention)
	// and set a busy timeout as cheap insurance.
	db, err := sql.Open(sqlite3, dbFileName)
	checkErr(err)
	defer db.Close()
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`PRAGMA busy_timeout = 5000;`)
	checkErr(err)

	// Single consumer => DB writes stay serialized, identical to before.
	for r := range results {
		checkErr(r.err)
		if !isHttpError(r.err) {
			podEpisodesIntoDatabase(db, r.podcast, r.episodes)
		}
	}
}

// init function is called automatically before main() in Go
// we use this to set up the logger and some globals
func init() {
	// logger
	log = logger.New(os.Stdout, os.Args[0]+" ", logger.LstdFlags|logger.Lshortfile)

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

// latestPodsFromDb lists the latest pods from the db
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
	  podcast_title,
	  IFNULL(published, first_seen),
	  podcastname_episodename_hash,
	  file
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
			&latest.dateForFilename,
			&latest.hash,
			&latest.file,
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

		fmt.Printf("%s / %s / %s / %s / %s\n",
			tsStr,
			nullStrToStr(latest.author),
			nullStrToStr(latest.podcast_title),
			nullStrToStr(latest.title),
			expectedFilenameForLatest(latest),
		)
	}
}

// expectedFilenameForLatest derives the filename gopodder would give this
// episode, matching what -s/--see writes to the download script. Episodes with
// no file (e.g. transcript-only entries) or no usable date can't be named, so
// we return "?" to stay consistent with nullStrToStr's convention.
func expectedFilenameForLatest(latest latestPodResult) string {
	if !latest.file.Valid || strings.TrimSpace(latest.file.String) == "" {
		return "?"
	}
	if !latest.dateForFilename.Valid || len([]rune(latest.dateForFilename.String)) < 10 {
		return "?"
	}
	return buildNonInteractiveFilename(
		nullStrToStr(latest.podcast_title),
		nullStrToStr(latest.title),
		latest.dateForFilename.String,
		latest.hash.String,
	)
}

// buildScanPaths returns the deduplicated list of directories that
// scanLocalPodFiles should consult: the primary podcasts dir first, followed
// by any extras from $GOPODDIR_ARCHIVES (PathListSeparator-separated). The
// primary dir is always included. Empty entries are skipped.
func buildScanPaths(podcastsDir, archivesEnv string) []string {
	paths := []string{podcastsDir}
	if strings.TrimSpace(archivesEnv) == "" {
		return paths
	}
	seen := map[string]bool{podcastsDir: true}
	for _, p := range strings.Split(archivesEnv, string(os.PathListSeparator)) {
		p = strings.TrimSpace(p)
		if p == "" || seen[p] {
			continue
		}
		seen[p] = true
		paths = append(paths, p)
	}
	return paths
}

func main() {
	// Get conf file path from env
	confFilePath, confVarIsSet := os.LookupEnv(confVarEnvName)
	podcastsDir, pathVarIsSet := os.LookupEnv(pathVarEnvName)
	archivesEnv, archivesVarIsSet := os.LookupEnv(archivesVarEnvName)

	// Use default if not set
	// we let the user know a bit further down
	if !confVarIsSet {
		confFilePath = confFilePathDefault
	}

	if !pathVarIsSet {
		podcastsDir = podcastsDirDefault
	}

	// Parse command line arguments

	// TODO: we should check for eyeD3 and wget availability
	// and update our help output accordingly
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
	-i will launch interactive mode

	Archiving (off-load older pods to another volume):
	--register-archive <dir>    Mark files in <dir> as archived; -s will not
	                            queue them for re-download even if <dir> is
	                            unmounted later.
	--unregister-archive <dir>  Inverse, e.g. when moving files back.
	--reconcile-archive         Drop registrations whose path no longer exists.
	$GOPODDIR_ARCHIVES          Colon-separated extra dirs to scan for already-
	                            downloaded files (mounted alternative to the
	                            registry above).

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
	interactiveMode := parser.Flag("i", "interactive", &argparse.Options{Required: false, Help: "Interactive episode picker"})

	registerArchiveOpt := parser.String("", "register-archive", &argparse.Options{Required: false, Help: "Register podcast files in <dir> as archived (won't be re-downloaded even if dir is unmounted)"})
	unregisterArchiveOpt := parser.String("", "unregister-archive", &argparse.Options{Required: false, Help: "Remove archive registrations matching files currently in <dir>"})
	reconcileArchiveOpt := parser.Flag("", "reconcile-archive", &argparse.Options{Required: false, Help: "Drop archive registrations whose archived path no longer resolves on disk"})

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
		log.Panic("Some issue with argparse")
	}

	// Given we know we have gone past the help message now let's
	// warn people if we are using defaults
	tmp_fmt := "%s is not set; using default, value is %s\n"
	if !confVarIsSet {
		log.Printf(tmp_fmt, confVarEnvName, confFilePath)
	}

	if !pathVarIsSet {
		log.Printf(tmp_fmt, pathVarEnvName, podcastsDir)
	}

	// Build the list of directories to scan when deciding what's already
	// downloaded: primary dir first, then any extras from GOPODDIR_ARCHIVES.
	scanPaths := buildScanPaths(podcastsDir, archivesEnv)
	if archivesVarIsSet && len(scanPaths) > 1 {
		log.Printf("%s adds %d archive scan path(s): %s", archivesVarEnvName, len(scanPaths)-1, strings.Join(scanPaths[1:], ", "))
	}

	// Check we have dependencies and get python path
	haveDependancies, pythonPath, eyeD3Dir := checkDependencies(verbose)

	log.Printf("Have dependencies: %t", haveDependancies)
	log.Printf("python path is %s", pythonPath)
	log.Printf("eyeD3 folder is %s", eyeD3Dir)

	// If we don't have dependencies then we exit
	if !haveDependancies {
		log.Panic("Exiting as we do not have dependancies")
	}

	// First let's get the tables ready to go and create them if not
	createTablesIfNotExist()

	// Archive-registry commands are independent of the parse/download pipeline.
	// Each one runs and exits — chaining with -p/-s/-d/-u/-t isn't supported.
	if r := strings.TrimSpace(*registerArchiveOpt); r != "" {
		n, err := registerArchiveDir(r)
		checkErr(err)
		log.Printf("registered %d archived episode(s) from %s", n, r)
		return
	}
	if u := strings.TrimSpace(*unregisterArchiveOpt); u != "" {
		n, err := unregisterArchiveDir(u)
		checkErr(err)
		log.Printf("unregistered %d archived episode(s) matching %s", n, u)
		return
	}
	if *reconcileArchiveOpt {
		n, err := reconcileArchiveRegistry()
		checkErr(err)
		log.Printf("removed %d stale archive registration(s)", n)
		return
	}

	// Interactive mode is exclusive from the parse/script pipeline
	if *interactiveMode {
		if err := runInteractive(podcastsDir, pythonPath, eyeD3Dir); err != nil {
			log.Panic(err)
		}
		return
	}

	cwd := getCwd()
	if *doAll || *downloadPods {
		// If we are downloading we make sure we are not downloading into home or similar
		cwdCheck(cwd)
	}

	if *doAll {
		parseThem(confFilePath)
		generateDownloadList(podcastsDir, scanPaths)
		runDownloadScript(podcastsDir)
		updateDatabaseForDownloads()
		tagThosePods(podcastsDir, pythonPath, eyeD3Dir)
	} else {
		if *parseOptPtr {
			parseThem(confFilePath)
		}

		if *seeOptPtr {
			generateDownloadList(podcastsDir, scanPaths)
		}

		if *downloadPods {
			runDownloadScript(podcastsDir)
		}

		if *postDlUpdate {
			updateDatabaseForDownloads()
		}

		if *tagPods {
			tagThosePods(podcastsDir, pythonPath, eyeD3Dir)
		}

		if *listLatestPods {
			latestPodsFromDb(confFilePath)
		}
	}
}
