package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"unicode"

	"github.com/forPelevin/gomoji"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

// checkErr is a utility function to checkErr err and also give line number of the calling function
func checkErr(err error) {
	// fine to crack on if err == nil
	if err == nil {
		return
	}

	// we want to print so get the line number
	_, _, line, _ := runtime.Caller(1)

	// if a http error then just print it
	if isHttpError(err) {
		log.Printf("%s (called from: %d)", err, line)
		// otherwise bork on non-http errors
	} else {
		log.Fatalf("%s (called from: %d)", err, line)
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
		log.Fatal("PATH does not seem to be set in environment")
	}

	pathDirs := strings.Split(path, ":")
	nPathDirs := len(pathDirs)

	for idx, dir := range pathDirs {
		if verbose {
			log.Printf("Checking PATH dir %d/%d: %s", idx+1, nPathDirs, dir)
		}
		filesInFolder, err := getExecutableFileNames(dir)

		if err != nil {
			if verbose {
				log.Printf("Error reading files in %s: %s", dir, err)
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
		log.Printf("%s found in %s", eyeD3, eyeD3Dir)
		eyeD3Path := eyeD3Dir + "/" + eyeD3
		pythonInterpreter = readFirstLine(eyeD3Path)
	}

	if haveWget && haveEyeD3 {
		log.Printf("Dependencies look good: have wget and %s", eyeD3)
		return true, pythonInterpreter, eyeD3Dir
	}

	log.Printf("PATH contains %d folders: %s", nPathDirs, path)
	if !haveWget {
		log.Println("FAIL: no wget")
		return false, pythonInterpreter, eyeD3Dir
	}

	if !haveEyeD3 {
		log.Printf("FAIL: no %s", eyeD3)
		return false, pythonInterpreter, eyeD3Dir
	}

	log.Println("FAIL: unknown reason")
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
