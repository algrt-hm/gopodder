package main

import (
	"crypto/md5"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEpisodeTimestamp(t *testing.T) {
	now := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	published := "2024-01-02T10:11:12Z"
	updated := "2024-02-03T01:02:03Z"

	got := episodeTimestamp(published, updated, now)
	want, err := time.Parse(time.RFC3339, published)
	if err != nil {
		t.Fatalf("parse published: %v", err)
	}
	if !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	got = episodeTimestamp("", updated, now)
	want, err = time.Parse(time.RFC3339, updated)
	if err != nil {
		t.Fatalf("parse updated: %v", err)
	}
	if !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	got = episodeTimestamp("", "", now)
	if !got.Equal(now) {
		t.Fatalf("got %v, want %v", got, now)
	}
}

func TestBuildEpisodeFilename(t *testing.T) {
	podcastTitle := "My Podcast"
	episodeTitle := "Hello World"
	dateStr := "2024-01-02"

	hash := fmt.Sprintf("%x", md5.Sum([]byte(podcastTitle+episodeTitle)))
	want := fmt.Sprintf(
		"%s-%s-%s-%s.%s",
		titleTransformation(podcastTitle),
		dateStr,
		titleTransformation(episodeTitle),
		hash,
		mp3,
	)

	got := buildEpisodeFilename(podcastTitle, episodeTitle, dateStr)
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestExpandHome(t *testing.T) {
	t.Setenv("HOME", "/tmp/home")

	got := expandHome("~")
	if got != "/tmp/home" {
		t.Fatalf("got %q, want %q", got, "/tmp/home")
	}

	got = expandHome("~/pods")
	if got != "/tmp/home/pods" {
		t.Fatalf("got %q, want %q", got, "/tmp/home/pods")
	}

	got = expandHome("/var/tmp")
	if got != "/var/tmp" {
		t.Fatalf("got %q, want %q", got, "/var/tmp")
	}

	got = expandHome("~other")
	if got != "~other" {
		t.Fatalf("got %q, want %q", got, "~other")
	}
}

func TestLoadExtraFeeds(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, extraConfName)
	content := "https://example.com/feed.rss\nnot a url\nhttp://example.org/other.xml\n"
	if err := os.WriteFile(path, []byte(content), 0666); err != nil {
		t.Fatalf("write extra conf: %v", err)
	}

	gotPath, gotFeeds, err, exists := loadExtraFeeds(dir)
	if err != nil {
		t.Fatalf("loadExtraFeeds error: %v", err)
	}
	if !exists {
		t.Fatalf("expected exists true")
	}
	if gotPath != path {
		t.Fatalf("got path %q, want %q", gotPath, path)
	}
	if len(gotFeeds) != 2 {
		t.Fatalf("got %d feeds, want 2", len(gotFeeds))
	}

	_, _, err, exists = loadExtraFeeds(filepath.Join(dir, "missing"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exists {
		t.Fatalf("expected exists false")
	}
}
