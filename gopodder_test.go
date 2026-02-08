package main

import (
	"crypto/md5"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mapset "github.com/deckarep/golang-set"
)

// TestCheckDependencies tests checkDependencies
// and also that the python interpreter path contains python
func TestCheckDependencies(t *testing.T) {
	gotStatus, gotPythonPath, gotEyeD3Path := checkDependencies(true)
	wantStatus := true

	if gotStatus != wantStatus || !strings.Contains(gotPythonPath, "python") || len(gotEyeD3Path) == 0 {
		t.Errorf("got status %+v, wanted status %+v", gotStatus, wantStatus)
		t.Errorf("got python path %+v, got eyeD3 path %+v", gotPythonPath, gotEyeD3Path)
	}
}

// TestCleanText tests cleanText with a couple of strings with less usual characters
func TestCleanText(t *testing.T) {
	s := "How €200bn of 'dirty money' flowed through a Danish bank Album: Behind the Money Genre: Podcast"
	want := "How e200bn of 'dirty money' flowed through a Danish bank Album: Behind the Money Genre: Podcast"
	got := cleanText(s, 1000)

	if got != want {
		t.Errorf("got %q, wanted %q", got, want)
	}

	s = "Is Turkey about to see the end of the Erdoğan era?"
	want = "Is Turkey about to see the end of the Erdogan era?"
	got = cleanText(s, 1000)

	if got != want {
		t.Errorf("got %q, wanted %q", got, want)
	}
}

func TestIsHttpError(t *testing.T) {
	err := fmt.Errorf("http error: 403 Forbidden")
	got := isHttpError(err)
	want := true
	if got != want {
		t.Errorf("got %v, wanted %v", got, want)
	}

	err = fmt.Errorf("http error: 500 Internal Server Error")
	got = isHttpError(err)
	want = true
	if got != want {
		t.Errorf("got %v, wanted %v", got, want)
	}

	err = fmt.Errorf("some random error")
	got = isHttpError(err)
	want = false
	if got != want {
		t.Errorf("got %v, wanted %v", got, want)
	}
}

func TestInteractiveAndNonInteractiveFilenamesMatch(t *testing.T) {
	now := time.Date(2025, 2, 3, 4, 5, 6, 0, time.UTC)
	podcastTitle := "Example Podcast"
	episodeTitle := "Episode 42"
	hash := fmt.Sprintf("%x", md5.Sum([]byte(podcastTitle+episodeTitle)))

	published := "2024-01-05T10:11:12Z"
	firstSeen := "2024-01-06T12:00:00Z"
	interactiveDate := episodeTimestamp(published, firstSeen, now).Format("2006-01-02")
	interactiveFilename := buildEpisodeFilenameWithHash(podcastTitle, episodeTitle, interactiveDate, hash)
	nonInteractiveFilename := buildNonInteractiveFilename(podcastTitle, episodeTitle, published, hash)
	if interactiveFilename != nonInteractiveFilename {
		t.Fatalf("published: got %q, want %q", interactiveFilename, nonInteractiveFilename)
	}

	published = ""
	interactiveDate = episodeTimestamp(published, firstSeen, now).Format("2006-01-02")
	interactiveFilename = buildEpisodeFilenameWithHash(podcastTitle, episodeTitle, interactiveDate, hash)
	nonInteractiveFilename = buildNonInteractiveFilename(podcastTitle, episodeTitle, firstSeen, hash)
	if interactiveFilename != nonInteractiveFilename {
		t.Fatalf("first_seen: got %q, want %q", interactiveFilename, nonInteractiveFilename)
	}
}

func TestHashFromFilenameValid(t *testing.T) {
	// Standard filename: PodTitle-2024-01-02-EpTitle-abc123.mp3
	// After "." → "-": PodTitle-2024-01-02-EpTitle-abc123-mp3
	// Split: [PodTitle, 2024, 01, 02, EpTitle, abc123, mp3]
	hash, tt, err := hashFromFilename("PodTitle-2024-01-02-EpTitle-abc123.mp3")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hash != "abc123" {
		t.Fatalf("got hash %q, want %q", hash, "abc123")
	}
	if tt != "EpTitle" {
		t.Fatalf("got transformedTitle %q, want %q", tt, "EpTitle")
	}
}

func TestHashFromFilenameErrors(t *testing.T) {
	cases := []struct {
		name     string
		filename string
	}{
		{"no dashes or dots", "nodashes"},
		{"single dot only", "nodashes.mp3"},   // becomes "nodashes-mp3" → 2 parts
		{"one dash only", "one-dash"},          // 2 parts
		{"empty string", ""},                   // 1 part
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := hashFromFilename(tc.filename)
			if err == nil {
				t.Fatalf("expected error for filename %q, got nil", tc.filename)
			}
		})
	}
}

func TestScanLocalPodFiles(t *testing.T) {
	dir := t.TempDir()

	// Valid podcast files (5 dashes in name, contains mp3)
	validFiles := []string{
		"PodTitle-2024-01-02-EpTitle-abc123.mp3",
		"Another_Pod-2024-06-15-Some_Episode-def456.mp3",
	}

	// Files that should be skipped
	invalidFiles := []string{
		"._PodTitle-2024-01-02-EpTitle-hidden.mp3", // starts with ._
		"wrong-dashes.mp3",                          // not 5 dashes
		"no-mp3-2024-01-02-title-hash.txt",          // no mp3
		"readme.txt",                                 // not a podcast
	}

	for _, f := range append(validFiles, invalidFiles...) {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("fake"), 0666); err != nil {
			t.Fatalf("create file %s: %v", f, err)
		}
	}

	hashSet, filenamesSet, ttsInFileNames, hashesToTT, ttToHashes := scanLocalPodFiles(dir)

	// Should have exactly 2 valid files
	if filenamesSet.Cardinality() != 2 {
		t.Fatalf("got %d filenames, want 2", filenamesSet.Cardinality())
	}
	for _, f := range validFiles {
		if !filenamesSet.Contains(f) {
			t.Errorf("expected %q in filenamesSet", f)
		}
	}

	// Should have 2 distinct hashes
	if hashSet.Cardinality() != 2 {
		t.Fatalf("got %d hashes, want 2", hashSet.Cardinality())
	}
	if !hashSet.Contains("abc123") {
		t.Error("expected hash abc123 in hashSet")
	}
	if !hashSet.Contains("def456") {
		t.Error("expected hash def456 in hashSet")
	}

	// Transformed titles
	if ttsInFileNames.Cardinality() != 2 {
		t.Fatalf("got %d transformed titles, want 2", ttsInFileNames.Cardinality())
	}

	// Map correctness
	if hashesToTT["abc123"] != "EpTitle" {
		t.Errorf("hashesToTT[abc123] = %q, want EpTitle", hashesToTT["abc123"])
	}
	if ttToHashes["EpTitle"] != "abc123" {
		t.Errorf("ttToHashes[EpTitle] = %q, want abc123", ttToHashes["EpTitle"])
	}
	if hashesToTT["def456"] != "Some_Episode" {
		t.Errorf("hashesToTT[def456] = %q, want Some_Episode", hashesToTT["def456"])
	}
}

func TestScanLocalPodFilesEmptyDir(t *testing.T) {
	dir := t.TempDir()
	hashSet, filenamesSet, ttsInFileNames, _, _ := scanLocalPodFiles(dir)

	if hashSet.Cardinality() != 0 {
		t.Errorf("expected 0 hashes, got %d", hashSet.Cardinality())
	}
	if filenamesSet.Cardinality() != 0 {
		t.Errorf("expected 0 filenames, got %d", filenamesSet.Cardinality())
	}
	if ttsInFileNames.Cardinality() != 0 {
		t.Errorf("expected 0 transformed titles, got %d", ttsInFileNames.Cardinality())
	}
}

func TestMatchByTransformedTitle(t *testing.T) {
	candidates := mapset.NewSet()
	candidates.Add("hash1")
	candidates.Add("hash2")
	candidates.Add("hash3")

	// Note: parameter is named ttToHashes but is used as hash→title in the function
	hashToTT := map[string]string{
		"hash1": "Episode_One",
		"hash2": "Episode_Two",
		"hash3": "Episode_Three",
	}

	// Local filenames — Episode_One appears as substring
	filenamesSlice := []interface{}{
		"SomePod-2024-01-02-Episode_One-oldhash.mp3",
	}

	// Transformed titles present in local filenames (backwards compat pass)
	ttsInFileNames := mapset.NewSet()
	ttsInFileNames.Add("Episode_Three")

	matchByTransformedTitle(candidates, hashToTT, filenamesSlice, ttsInFileNames)

	// hash1: removed by substring match (Episode_One is in filename)
	if candidates.Contains("hash1") {
		t.Error("hash1 should have been removed by substring match")
	}
	// hash2: no match, should remain
	if !candidates.Contains("hash2") {
		t.Error("hash2 should remain (no match)")
	}
	// hash3: removed by exact title match in ttsInFileNames
	if candidates.Contains("hash3") {
		t.Error("hash3 should have been removed by exact title match")
	}
}

func TestMatchByTransformedTitleEmptyCandidates(t *testing.T) {
	candidates := mapset.NewSet()
	hashToTT := map[string]string{}
	filenamesSlice := []interface{}{"some-2024-01-02-file-hash.mp3"}
	ttsInFileNames := mapset.NewSet()

	// Should not panic on empty candidates
	matchByTransformedTitle(candidates, hashToTT, filenamesSlice, ttsInFileNames)

	if candidates.Cardinality() != 0 {
		t.Errorf("expected 0 candidates, got %d", candidates.Cardinality())
	}
}

func TestCheckErrPanicsOnNonHttpError(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected checkErr to panic on non-HTTP error")
		}
	}()
	checkErr(fmt.Errorf("test database error"))
}

func TestCheckErrNilIsNoop(t *testing.T) {
	// Should not panic
	checkErr(nil)
}

func TestCheckErrHttpErrorDoesNotPanic(t *testing.T) {
	// HTTP errors should be logged but not cause a panic
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("checkErr should not panic on HTTP error, got: %v", r)
		}
	}()
	checkErr(fmt.Errorf("http error: 404 Not Found"))
}
