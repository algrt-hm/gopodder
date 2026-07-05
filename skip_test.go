package main

import "testing"

func TestLongestCommonSubstringLen(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"", "anything", 0},
		{"abc", "abc", 3},
		{"xabcy", "zabcw", 3},
		{"abc", "def", 0},
		{"vlad tenev", "inside the mind of vlad tenev", 10},
	}
	for _, c := range cases {
		if got := longestCommonSubstringLen(c.a, c.b); got != c.want {
			t.Errorf("longestCommonSubstringLen(%q, %q) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

func TestMaterialTitleOverlap(t *testing.T) {
	cases := []struct {
		name, a, b string
		want       bool
	}{
		{
			// Real Knowledge Project retitle pair (guid-stable in practice,
			// but the fallback should also recognise it)
			"mild retitle",
			"Ai Goes  Parabolic | Greg Brockman, Co-Founder OpenAI",
			"Ai Goes Parabolic | OpenAI Co-Founder Greg Brockman",
			true,
		},
		{
			// Episode-number prefix dropped (Hidden Forces style) — digit
			// sequences differ, so the guard refuses; the guid rule is what
			// handles these
			"numbered retitle blocked by digit guard",
			"Ep.384 Moving From an Income-Driven to a Credit-Driven Cycle | Bob Elliott",
			"Moving From an Income-Driven to a Credit-Driven Cycle | Bob Elliott",
			false,
		},
		{
			// "The Deobandis" case: same show, same day, distinct parts
			"part 1 vs part 2",
			"The Deobandis Part 1",
			"The Deobandis Part 2",
			false,
		},
		{
			// Distinct same-day episodes of a daily feed share a word; a flat
			// >=5-char rule would wrongly merge these
			"daily feed distinct episodes",
			"US tariffs hit markets",
			"Markets rally as tariffs pause",
			false,
		},
		{
			"different guests same day",
			"Matt Lindland: Exclusive Interview",
			"Michael Thomsen: Cage Kings",
			false,
		},
		{
			// Series episodes share a title template; the word-set path must
			// not merge them (this pair scores Jaccard 0.625)
			"series title template",
			"The Rise and Fall of the House of Murdoch",
			"The Rise and Fall of Enron",
			false,
		},
	}
	for _, c := range cases {
		if got := materialTitleOverlap(c.a, c.b); got != c.want {
			t.Errorf("%s: materialTitleOverlap(%q, %q) = %v, want %v", c.name, c.a, c.b, got, c.want)
		}
	}
}

func TestPlanDownloadSkipsGuidSiblingAlreadyDownloaded(t *testing.T) {
	// The 2026-07-05 incident: retitled row pending, older title on disk
	cands := []downloadCandidate{
		{podcastTitle: "The Knowledge Project", published: "2026-03-03T09:00:00Z",
			title: "Inside the Mind of Robinhood Co-Founder Vlad Tenev",
			episodeHash: "aaa", guid: "guid-tenev", lastSeen: "2026-04-21T00:00:00Z", have: true},
		{podcastTitle: "The Knowledge Project", published: "2026-03-03T09:00:00Z",
			title: "The Near Death Experience of RobinHood | Vlad Tenev, Co-Founder",
			episodeHash: "bbb", guid: "guid-tenev", lastSeen: "2026-07-05T00:00:00Z", have: false},
	}
	skips := planDownloadSkips(cands)
	s, ok := skips["bbb"]
	if !ok {
		t.Fatal("expected pending retitle row to be skipped")
	}
	if s.matchedHash != "aaa" {
		t.Errorf("matchedHash = %q, want aaa", s.matchedHash)
	}
	if _, ok := skips["aaa"]; ok {
		t.Error("already-downloaded row must not be skipped")
	}
}

func TestPlanDownloadSkipsGuidPendingPair(t *testing.T) {
	// Episode retitled before we ever downloaded it: two pending rows, one
	// guid — only the fresher row (the title the feed uses now) downloads
	cands := []downloadCandidate{
		{podcastTitle: "P", published: "2026-06-01T00:00:00Z", title: "Old Title",
			episodeHash: "old", guid: "g1", lastSeen: "2026-06-10T00:00:00Z", have: false},
		{podcastTitle: "P", published: "2026-06-01T00:00:00Z", title: "New Title",
			episodeHash: "new", guid: "g1", lastSeen: "2026-07-05T00:00:00Z", have: false},
	}
	skips := planDownloadSkips(cands)
	if s, ok := skips["old"]; !ok || s.matchedHash != "new" {
		t.Errorf("stale pending row should defer to fresher sibling, got %+v", skips)
	}
	if _, ok := skips["new"]; ok {
		t.Error("fresher pending row must not be skipped")
	}
}

func TestPlanDownloadSkipsGuidAcrossPodcastsIndependent(t *testing.T) {
	// Same guid string under two different podcasts must not interact
	cands := []downloadCandidate{
		{podcastTitle: "A", published: "2026-06-01T00:00:00Z", title: "T",
			episodeHash: "a1", guid: "shared", have: true},
		{podcastTitle: "B", published: "2026-06-01T00:00:00Z", title: "Unrelated",
			episodeHash: "b1", guid: "shared", have: false},
	}
	if skips := planDownloadSkips(cands); len(skips) != 0 {
		t.Errorf("expected no skips across podcasts, got %+v", skips)
	}
}

func TestPlanDownloadSkipsTitleFallback(t *testing.T) {
	// Guid-rotating feed: same podcast, same date, overlapping title
	cands := []downloadCandidate{
		{podcastTitle: "P", published: "2026-05-01T00:00:00Z",
			title:       "The Magic of Thinking Big | XPO CEO Mario Harik",
			episodeHash: "have1", guid: "g-old", have: true},
		{podcastTitle: "P", published: "2026-05-01T00:00:00Z",
			title:       "The Magic of Thinking Big: Mario Harik",
			episodeHash: "pend1", guid: "g-new", have: false},
	}
	skips := planDownloadSkips(cands)
	if s, ok := skips["pend1"]; !ok || s.matchedHash != "have1" {
		t.Errorf("expected title-fallback skip against have1, got %+v", skips)
	}
}

func TestPlanDownloadSkipsDistinctSameDayEpisodes(t *testing.T) {
	// Same show, same day, genuinely different episodes: no skip
	cands := []downloadCandidate{
		{podcastTitle: "A Book with Legs", published: "2023-08-21T00:00:00Z",
			title:       "Matt Lindland: Exclusive Interview",
			episodeHash: "h1", guid: "g1", have: true},
		{podcastTitle: "A Book with Legs", published: "2023-08-21T00:00:00Z",
			title:       "Michael Thomsen: Cage Kings",
			episodeHash: "h2", guid: "g2", have: false},
	}
	if skips := planDownloadSkips(cands); len(skips) != 0 {
		t.Errorf("expected no skips for distinct same-day episodes, got %+v", skips)
	}
}

func TestPlanDownloadSkipsPartSiblings(t *testing.T) {
	// Digit guard: Part 2 pending while Part 1 is on disk must download
	cands := []downloadCandidate{
		{podcastTitle: "Analysis", published: "2016-04-01T00:00:00Z",
			title:       "The Deobandis Part 1",
			episodeHash: "p1", guid: "gp1", have: true},
		{podcastTitle: "Analysis", published: "2016-04-01T00:00:00Z",
			title:       "The Deobandis Part 2",
			episodeHash: "p2", guid: "gp2", have: false},
	}
	if skips := planDownloadSkips(cands); len(skips) != 0 {
		t.Errorf("expected no skips for part siblings, got %+v", skips)
	}
}
