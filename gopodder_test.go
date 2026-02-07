package main

import (
	"crypto/md5"
	"fmt"
	"strings"
	"testing"
	"time"
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
