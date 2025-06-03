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
var log *logger.Logger // Logger
var ts string          // This will be starting timestamp
var confFilePathDefault string
var podcastsDirDefault string
var verbose bool = false // Verbosity. TODO: this should use the logger and loglevels
var cwd string           // Current working directory

// TODO: Should refactor so these are not globals
var pythonInterpreterPath string
var eyeD3Path string

// M is an alias for map[string]interface{}
type M map[string]interface{}

// readConfig is a function which reads the configuration file and return slices with URLs for RSS files
func readConfig(confFilePath string) ([]string, error) {
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

// hashFromFilename returns the hash part from the filename
// we can then compare this hash to what is in the db
// TODO: add example filename
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
			log.Fatal("No tTitle")
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

// generateDownloadList generates download script based on the pods to download
func generateDownloadList(podcastsDir string) {
	hashes := seeWhatPodsWeAlreadyHave(dbFileName, podcastsDir)

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
	err = os.WriteFile(filename, []byte(linesJoined), 0666)
	checkErr(err)

	log.Printf("Written script to %s", filename)
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
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	checkErr(err)

	fmt.Printf("Removing tags with %s:", eyeD3)
	cmd = exec.Command(pythonInterpreterPath, eyeD3Path, "--remove-all", filename)
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

// tagSinglePod tags the file at filename with the title and album metadata
func tagSinglePod(filename string, title string, album string) {

	// title tag is title
	// album is podcast title
	// genre is "Podcast"
	const genre = "Podcast"
	var parse bool = true

	tag, err := id3v2.Open(filename, id3v2.Options{Parse: parse})

	// if we fail to open we run eyeD3 to remove any tags and try again
	if err != nil {
		fmt.Printf("%s is a file where reading the tags has been problematic %s\n", filename, err)
		stripTagsWithEyeD3(filename, pythonInterpreterPath, eyeD3Path)
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
		log.Printf("%s: %s / %s", filename, podcast_title, title)

		// Tag 'em
		tagSinglePod(filename, title, podcast_title)
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
				log.Fatal("More than one row affected which should not happen")
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
		if !isHttpError(err) {
			podEpisodesIntoDatabase(podcast, episodes)
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
		log.Fatal("Some issue with argparse")
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

	// Check we have dependencies and get python path
	haveDependancies, python, eyeD3Dir := checkDependencies(verbose)

	// Set globals
	// TODO: globals should be refactored out
	pythonInterpreterPath = python
	eyeD3Path = eyeD3Dir

	log.Printf("Have dependencies: %t", haveDependancies)
	log.Printf("python path is %s", pythonInterpreterPath)
	log.Printf("eyeD3 folder is %s", eyeD3Path)

	// If we don't have dependencies then we exit
	if !haveDependancies {
		log.Fatal("Exiting as we do not have dependancies")
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
