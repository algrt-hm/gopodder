package main

import (
	"crypto/md5"
	"database/sql"
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

func TestGetCwdReturnsProcessWorkingDirectory(t *testing.T) {
	tmpDir := useTempWorkingDir(t)

	got, err := filepath.EvalSymlinks(getCwd())
	if err != nil {
		t.Fatalf("EvalSymlinks(getCwd): %v", err)
	}
	want, err := filepath.EvalSymlinks(tmpDir)
	if err != nil {
		t.Fatalf("EvalSymlinks(tmpDir): %v", err)
	}
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
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
		{"single dot only", "nodashes.mp3"}, // becomes "nodashes-mp3" → 2 parts
		{"one dash only", "one-dash"},       // 2 parts
		{"empty string", ""},                // 1 part
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
		"wrong-dashes.mp3",                         // not 5 dashes
		"no-mp3-2024-01-02-title-hash.txt",         // no mp3
		"readme.txt",                               // not a podcast
	}

	for _, f := range append(validFiles, invalidFiles...) {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("fake"), 0666); err != nil {
			t.Fatalf("create file %s: %v", f, err)
		}
	}

	hashSet, filenamesSet, ttsInFileNames, hashesToTT, ttToHashes := scanLocalPodFiles([]string{dir})

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
	hashSet, filenamesSet, ttsInFileNames, _, _ := scanLocalPodFiles([]string{dir})

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

func TestGenerateDownloadListKeepsNewEpisodesWithCollidingTransformedTitles(t *testing.T) {
	tmpDir := useTempWorkingDir(t)
	createTablesIfNotExist()

	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	podcastTitle := "Numbers Show"
	oldTitle := "Episode 235"
	newTitle := "Episode 236"

	oldEpisodeHash := fmt.Sprintf("%x", md5.Sum([]byte(podcastTitle+oldTitle)))
	newEpisodeHash := fmt.Sprintf("%x", md5.Sum([]byte(podcastTitle+newTitle)))
	oldFileURL := "https://example.com/235.mp3"
	newFileURL := "https://example.com/236.mp3"
	oldFileURLHash := fmt.Sprintf("%x", md5.Sum([]byte(oldFileURL)))
	newFileURLHash := fmt.Sprintf("%x", md5.Sum([]byte(newFileURL)))

	oldFilename := buildEpisodeFilenameWithHash(podcastTitle, oldTitle, "2024-01-01", oldEpisodeHash)
	if err := os.WriteFile(filepath.Join(tmpDir, oldFilename), []byte("existing"), 0666); err != nil {
		t.Fatalf("write existing file: %v", err)
	}

	_, err = db.Exec(`
		INSERT INTO episodes (
			title, published, first_seen, last_seen, podcast_title,
			podcastname_episodename_hash, file_url_hash, file
		) VALUES
			(?, ?, ?, ?, ?, ?, ?, ?),
			(?, ?, ?, ?, ?, ?, ?, ?)
		;`,
		oldTitle, "2024-01-01T00:00:00Z", ts, ts, podcastTitle, oldEpisodeHash, oldFileURLHash, oldFileURL,
		newTitle, "2024-01-02T00:00:00Z", ts, ts, podcastTitle, newEpisodeHash, newFileURLHash, newFileURL,
	)
	if err != nil {
		t.Fatalf("insert episodes: %v", err)
	}

	generateDownloadList(tmpDir, []string{tmpDir})

	scriptPath := filepath.Join(tmpDir, "download_pods.sh")
	script, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatalf("read download script: %v", err)
	}

	scriptText := string(script)
	if !strings.Contains(scriptText, newFileURL) {
		t.Fatalf("expected download script to include new episode URL %q, got %q", newFileURL, scriptText)
	}
	if strings.Contains(scriptText, oldFileURL) {
		t.Fatalf("expected download script to exclude existing episode URL %q, got %q", oldFileURL, scriptText)
	}
}

func TestLegacyURLHashFilenamesExcludedFromDownloadList(t *testing.T) {
	tmpDir := useTempWorkingDir(t)
	createTablesIfNotExist()

	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	podcastTitle := "Legacy Show"
	legacyTitle := "Old Episode"
	freshTitle := "New Episode"

	legacyEpisodeHash := fmt.Sprintf("%x", md5.Sum([]byte(podcastTitle+legacyTitle)))
	freshEpisodeHash := fmt.Sprintf("%x", md5.Sum([]byte(podcastTitle+freshTitle)))
	legacyURL := "https://example.com/old.mp3"
	freshURL := "https://example.com/new.mp3"
	legacyURLHash := fmt.Sprintf("%x", md5.Sum([]byte(legacyURL)))
	freshURLHash := fmt.Sprintf("%x", md5.Sum([]byte(freshURL)))

	// The legacy episode exists on disk under the old filename scheme:
	// named with the file-URL hash, not the episode hash.
	legacyFilename := buildEpisodeFilenameWithHash(podcastTitle, legacyTitle, "2020-01-01", legacyURLHash)
	if err := os.WriteFile(filepath.Join(tmpDir, legacyFilename), []byte("existing"), 0666); err != nil {
		t.Fatalf("write legacy file: %v", err)
	}

	// A new-scheme file must also be present, otherwise the transformed-title
	// fallback kicks in and masks the hash-matching path under test.
	otherTitle := "Other Episode"
	otherEpisodeHash := fmt.Sprintf("%x", md5.Sum([]byte(podcastTitle+otherTitle)))
	otherURL := "https://example.com/other.mp3"
	otherURLHash := fmt.Sprintf("%x", md5.Sum([]byte(otherURL)))
	otherFilename := buildEpisodeFilenameWithHash(podcastTitle, otherTitle, "2023-01-01", otherEpisodeHash)
	if err := os.WriteFile(filepath.Join(tmpDir, otherFilename), []byte("existing"), 0666); err != nil {
		t.Fatalf("write other file: %v", err)
	}

	_, err = db.Exec(`
		INSERT INTO episodes (
			title, published, first_seen, last_seen, podcast_title,
			podcastname_episodename_hash, file_url_hash, file
		) VALUES
			(?, ?, ?, ?, ?, ?, ?, ?),
			(?, ?, ?, ?, ?, ?, ?, ?),
			(?, ?, ?, ?, ?, ?, ?, ?)
		;`,
		legacyTitle, "2020-01-01T00:00:00Z", ts, ts, podcastTitle, legacyEpisodeHash, legacyURLHash, legacyURL,
		otherTitle, "2023-01-01T00:00:00Z", ts, ts, podcastTitle, otherEpisodeHash, otherURLHash, otherURL,
		freshTitle, "2024-01-02T00:00:00Z", ts, ts, podcastTitle, freshEpisodeHash, freshURLHash, freshURL,
	)
	if err != nil {
		t.Fatalf("insert episodes: %v", err)
	}

	generateDownloadList(tmpDir, []string{tmpDir})

	script, err := os.ReadFile(filepath.Join(tmpDir, "download_pods.sh"))
	if err != nil {
		t.Fatalf("read download script: %v", err)
	}
	text := string(script)
	if !strings.Contains(text, freshURL) {
		t.Fatalf("expected fresh URL %q in download script, got %q", freshURL, text)
	}
	if strings.Contains(text, legacyURL) {
		t.Fatalf("expected legacy-named episode URL %q to be EXCLUDED from download script, got %q", legacyURL, text)
	}
}

func TestLegacyURLHashInArchiveRegistryExcludedFromDownloadList(t *testing.T) {
	tmpDir := useTempWorkingDir(t)
	createTablesIfNotExist()

	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	podcastTitle := "Legacy Archive Show"
	archivedTitle := "Archived Legacy Episode"
	freshTitle := "Fresh Episode"

	archivedEpisodeHash := fmt.Sprintf("%x", md5.Sum([]byte(podcastTitle+archivedTitle)))
	freshEpisodeHash := fmt.Sprintf("%x", md5.Sum([]byte(podcastTitle+freshTitle)))
	archivedURL := "https://example.com/archived-legacy.mp3"
	freshURL := "https://example.com/fresh.mp3"
	archivedURLHash := fmt.Sprintf("%x", md5.Sum([]byte(archivedURL)))
	freshURLHash := fmt.Sprintf("%x", md5.Sum([]byte(freshURL)))

	if _, err := db.Exec(`
		INSERT INTO episodes (
			title, published, first_seen, last_seen, podcast_title,
			podcastname_episodename_hash, file_url_hash, file
		) VALUES
			(?, ?, ?, ?, ?, ?, ?, ?),
			(?, ?, ?, ?, ?, ?, ?, ?)
		;`,
		archivedTitle, "2020-01-01T00:00:00Z", ts, ts, podcastTitle, archivedEpisodeHash, archivedURLHash, archivedURL,
		freshTitle, "2024-01-02T00:00:00Z", ts, ts, podcastTitle, freshEpisodeHash, freshURLHash, freshURL,
	); err != nil {
		t.Fatalf("insert episodes: %v", err)
	}

	// Registering a legacy-named archived file stores the URL hash parsed
	// from its filename, not the episode hash.
	if _, err := db.Exec(`
		INSERT INTO archived_episodes (podcastname_episodename_hash, archived_path, archived_at)
		VALUES (?, ?, ?)
		;`, archivedURLHash, "/some/archive/path/archived-legacy.mp3", ts); err != nil {
		t.Fatalf("insert archived row: %v", err)
	}

	generateDownloadList(tmpDir, []string{tmpDir})

	script, err := os.ReadFile(filepath.Join(tmpDir, "download_pods.sh"))
	if err != nil {
		t.Fatalf("read script: %v", err)
	}
	text := string(script)
	if !strings.Contains(text, freshURL) {
		t.Fatalf("expected fresh URL %q in download script, got %q", freshURL, text)
	}
	if strings.Contains(text, archivedURL) {
		t.Fatalf("expected legacy-hash archived URL %q to be EXCLUDED from download script, got %q", archivedURL, text)
	}
}

func TestScanLocalPodFilesUnionsAcrossPaths(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()

	if err := os.WriteFile(filepath.Join(dirA, "PodA-2024-01-02-Ep-aaa111.mp3"), []byte("a"), 0666); err != nil {
		t.Fatalf("write A: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dirB, "PodB-2024-01-02-Ep-bbb222.mp3"), []byte("b"), 0666); err != nil {
		t.Fatalf("write B: %v", err)
	}

	hashSet, filenamesSet, _, _, _ := scanLocalPodFiles([]string{dirA, dirB})
	if hashSet.Cardinality() != 2 {
		t.Fatalf("got %d hashes, want 2", hashSet.Cardinality())
	}
	if !hashSet.Contains("aaa111") || !hashSet.Contains("bbb222") {
		t.Fatalf("expected both hashes present, got %v", hashSet)
	}
	if filenamesSet.Cardinality() != 2 {
		t.Fatalf("got %d filenames, want 2", filenamesSet.Cardinality())
	}
}

func TestBuildScanPaths(t *testing.T) {
	cases := []struct {
		name        string
		podcastsDir string
		archivesEnv string
		want        []string
	}{
		{"empty env returns just primary", "/p", "", []string{"/p"}},
		{"single extra path", "/p", "/a", []string{"/p", "/a"}},
		{"multiple paths", "/p", "/a:/b:/c", []string{"/p", "/a", "/b", "/c"}},
		{"dedupe primary", "/p", "/p:/a", []string{"/p", "/a"}},
		{"dedupe extras", "/p", "/a:/a:/b", []string{"/p", "/a", "/b"}},
		{"empty entries skipped", "/p", ":/a::", []string{"/p", "/a"}},
		{"whitespace trimmed", "/p", " /a : /b ", []string{"/p", "/a", "/b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := buildScanPaths(tc.podcastsDir, tc.archivesEnv)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("idx %d: got %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestArchivedEpisodesExcludedFromDownloadList(t *testing.T) {
	tmpDir := useTempWorkingDir(t)
	createTablesIfNotExist()

	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	podcastTitle := "Archive Test Show"
	archivedEpTitle := "Archived Episode"
	freshEpTitle := "Fresh Episode"

	archivedHash := fmt.Sprintf("%x", md5.Sum([]byte(podcastTitle+archivedEpTitle)))
	freshHash := fmt.Sprintf("%x", md5.Sum([]byte(podcastTitle+freshEpTitle)))
	archivedURL := "https://example.com/archived.mp3"
	freshURL := "https://example.com/fresh.mp3"
	archivedURLHash := fmt.Sprintf("%x", md5.Sum([]byte(archivedURL)))
	freshURLHash := fmt.Sprintf("%x", md5.Sum([]byte(freshURL)))

	if _, err := db.Exec(`
		INSERT INTO episodes (
			title, published, first_seen, last_seen, podcast_title,
			podcastname_episodename_hash, file_url_hash, file
		) VALUES
			(?, ?, ?, ?, ?, ?, ?, ?),
			(?, ?, ?, ?, ?, ?, ?, ?)
		;`,
		archivedEpTitle, "2020-01-01T00:00:00Z", ts, ts, podcastTitle, archivedHash, archivedURLHash, archivedURL,
		freshEpTitle, "2024-01-02T00:00:00Z", ts, ts, podcastTitle, freshHash, freshURLHash, freshURL,
	); err != nil {
		t.Fatalf("insert episodes: %v", err)
	}

	if _, err := db.Exec(`
		INSERT INTO archived_episodes (podcastname_episodename_hash, archived_path, archived_at)
		VALUES (?, ?, ?)
		;`, archivedHash, "/some/archive/path/archived.mp3", ts); err != nil {
		t.Fatalf("insert archived row: %v", err)
	}

	// Neither episode has a local file in tmpDir.
	generateDownloadList(tmpDir, []string{tmpDir})

	scriptText, err := os.ReadFile(filepath.Join(tmpDir, "download_pods.sh"))
	if err != nil {
		t.Fatalf("read script: %v", err)
	}
	text := string(scriptText)
	if !strings.Contains(text, freshURL) {
		t.Fatalf("expected fresh URL %q in download script, got %q", freshURL, text)
	}
	if strings.Contains(text, archivedURL) {
		t.Fatalf("expected archived URL %q to be EXCLUDED from download script, got %q", archivedURL, text)
	}
}

func TestRegisterAndUnregisterArchiveDir(t *testing.T) {
	useTempWorkingDir(t)
	createTablesIfNotExist()

	archiveDir := t.TempDir()
	for _, name := range []string{
		"PodA-2020-01-02-Ep_One-aaa111.mp3",
		"PodA-2020-02-03-Ep_Two-bbb222.mp3",
		"not-a-podcast.txt",
		"._PodA-2020-03-04-Hidden-ccc333.mp3",
	} {
		if err := os.WriteFile(filepath.Join(archiveDir, name), []byte("x"), 0666); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	n, err := registerArchiveDir(archiveDir)
	if err != nil {
		t.Fatalf("registerArchiveDir: %v", err)
	}
	if n != 2 {
		t.Fatalf("registered %d, want 2", n)
	}

	hashes := fetchArchivedHashes()
	if !hashes.Contains("aaa111") || !hashes.Contains("bbb222") {
		t.Fatalf("expected both hashes registered, got %v", hashes)
	}
	if hashes.Cardinality() != 2 {
		t.Fatalf("got %d archived hashes, want 2", hashes.Cardinality())
	}

	// Re-running register on the same dir should refresh, not duplicate.
	n, err = registerArchiveDir(archiveDir)
	if err != nil {
		t.Fatalf("second registerArchiveDir: %v", err)
	}
	if n != 2 {
		t.Fatalf("re-registered %d, want 2", n)
	}
	if fetchArchivedHashes().Cardinality() != 2 {
		t.Fatalf("expected still 2 archived hashes after re-register")
	}

	// Unregister.
	n, err = unregisterArchiveDir(archiveDir)
	if err != nil {
		t.Fatalf("unregisterArchiveDir: %v", err)
	}
	if n != 2 {
		t.Fatalf("unregistered %d, want 2", n)
	}
	if fetchArchivedHashes().Cardinality() != 0 {
		t.Fatalf("expected 0 archived hashes after unregister")
	}
}

func TestReconcileArchiveRegistry(t *testing.T) {
	useTempWorkingDir(t)
	createTablesIfNotExist()

	archiveDir := t.TempDir()
	stillThere := "PodA-2020-01-02-Still_There-aaa111.mp3"
	if err := os.WriteFile(filepath.Join(archiveDir, stillThere), []byte("x"), 0666); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if _, err := registerArchiveDir(archiveDir); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Inject a registry row pointing at a path that doesn't exist.
	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	missingPath := filepath.Join(archiveDir, "definitely-not-here.mp3")
	if _, err := db.Exec(`
		INSERT INTO archived_episodes (podcastname_episodename_hash, archived_path, archived_at)
		VALUES (?, ?, ?)
		;`, "ghost123", missingPath, ts); err != nil {
		t.Fatalf("insert ghost: %v", err)
	}

	removed, err := reconcileArchiveRegistry()
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if removed != 1 {
		t.Fatalf("removed %d, want 1", removed)
	}
	hashes := fetchArchivedHashes()
	if hashes.Contains("ghost123") {
		t.Fatalf("ghost row should have been removed")
	}
	if !hashes.Contains("aaa111") {
		t.Fatalf("present-on-disk row should have been kept")
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
