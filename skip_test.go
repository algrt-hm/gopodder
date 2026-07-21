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
		{
			// 2026-07-20: World Service edition prefix — too short for
			// containment (14 runes) and too few words for the word-set
			// path; the prefix-strip path must recognise it
			"ws edition prefix",
			"Climate Change",
			"WS MoreOrLess: Climate Change",
			true,
		},
		{
			// Spelled-out number vs digit ("Part One" / "Part 1")
			"number word vs digit",
			"Numbers of the Year Part One",
			"WS More or Less: Numbers of the Year Part 1",
			true,
		},
		{
			// Spelled-out part siblings must hit the digit guard exactly
			// like "Part 1" vs "Part 2" does
			"number word part siblings",
			"Numbers of the year: Part one",
			"Numbers of the year: Part two",
			false,
		},
		{
			// A preview is not another edition of the full episode
			"preview prefix not stripped",
			"Preview: The Fall of Rome",
			"The Fall of Rome",
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
			title:       "Inside the Mind of Robinhood Co-Founder Vlad Tenev",
			episodeHash: "aaa", guid: "guid-tenev", lastSeen: "2026-04-21T00:00:00Z", have: true},
		{podcastTitle: "The Knowledge Project", published: "2026-03-03T09:00:00Z",
			title:       "The Near Death Experience of RobinHood | Vlad Tenev, Co-Founder",
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
	// Same guid string under two different podcasts must not interact when
	// the titles don't corroborate (junk guids like "1" or a date can
	// collide between unrelated feeds)
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

func TestPlanDownloadSkipsCrossPodcastRename(t *testing.T) {
	// The 2026-07-09 incident: BBC retitled "Arts & Ideas" to "Free
	// Thinking"; every episode re-hashed as new under the new podcast title
	// while its copy sat on disk under the old one. Same guid plus the same
	// title across podcasts must skip.
	cands := []downloadCandidate{
		{podcastTitle: "Arts & Ideas", published: "2026-07-03T15:00:00Z",
			title:       "Trade and traffic",
			episodeHash: "old1", guid: "urn:bbc:podcast:p0nwsdfz", have: true},
		{podcastTitle: "Free Thinking", published: "2026-07-03T15:00:00Z",
			title:       "Trade and traffic",
			episodeHash: "new1", guid: "urn:bbc:podcast:p0nwsdfz", have: false},
	}
	skips := planDownloadSkips(cands)
	s, ok := skips["new1"]
	if !ok {
		t.Fatal("expected renamed-podcast row to be skipped")
	}
	if s.matchedHash != "old1" {
		t.Errorf("matchedHash = %q, want old1", s.matchedHash)
	}
	if _, ok := skips["old1"]; ok {
		t.Error("already-downloaded row must not be skipped")
	}
}

func TestPlanDownloadSkipsCrossPodcastGuidNoFileNoSkip(t *testing.T) {
	// A same-guid same-title row of another podcast that has NO file gives
	// us nothing to point at — the pending row must still download
	cands := []downloadCandidate{
		{podcastTitle: "Arts & Ideas", published: "2026-07-03T15:00:00Z",
			title:       "Trade and traffic",
			episodeHash: "old1", guid: "urn:bbc:podcast:p0nwsdfz", have: false},
		{podcastTitle: "Free Thinking", published: "2026-07-03T15:00:00Z",
			title:       "Trade and traffic",
			episodeHash: "new1", guid: "urn:bbc:podcast:p0nwsdfz", have: false},
	}
	if skips := planDownloadSkips(cands); len(skips) != 0 {
		t.Errorf("expected no skips when neither copy has a file, got %+v", skips)
	}
}

func TestPlanDownloadSkipsSamePodcastGuidRuleWinsOverRename(t *testing.T) {
	// When both a same-podcast and a cross-podcast guid sibling have files,
	// the same-podcast one is the better record to point at
	cands := []downloadCandidate{
		{podcastTitle: "Free Thinking", published: "2026-07-03T15:00:00Z",
			title:       "Trade and traffic",
			episodeHash: "sameA", guid: "g", have: true},
		{podcastTitle: "Arts & Ideas", published: "2026-07-03T15:00:00Z",
			title:       "Trade and traffic",
			episodeHash: "crossB", guid: "g", have: true},
		{podcastTitle: "Free Thinking", published: "2026-07-03T15:00:00Z",
			title:       "Trade and traffic (extended)",
			episodeHash: "pend", guid: "g", have: false},
	}
	skips := planDownloadSkips(cands)
	if s, ok := skips["pend"]; !ok || s.matchedHash != "sameA" {
		t.Errorf("expected same-podcast guid sibling to win, got %+v", skips)
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

func TestPlanDownloadSkipsRepeatIdenticalTitleAnyDate(t *testing.T) {
	// BBC repeat: same episode re-enters the feed with a fresh guid and the
	// re-broadcast date. Identical title + a downloaded sibling must skip
	// regardless of date (rule 3b) — this was the 45-episode residual tail
	// of the 2026-07-09 rename incident.
	cands := []downloadCandidate{
		{podcastTitle: "Free Thinking", published: "2023-06-15T16:00:00Z",
			title:       "Hitchhiking",
			episodeHash: "orig", guid: "urn:bbc:podcast:p0bpr86s", have: true},
		{podcastTitle: "Free Thinking", published: "2024-02-28T17:00:00Z",
			title:       "Hitchhiking",
			episodeHash: "repeat", guid: "urn:bbc:podcast:p0hffcg6", have: false},
	}
	skips := planDownloadSkips(cands)
	if s, ok := skips["repeat"]; !ok || s.matchedHash != "orig" {
		t.Errorf("expected repeat to skip against original, got %+v", skips)
	}
}

func TestPlanDownloadSkipsSimilarTitleDifferentDateNotSkipped(t *testing.T) {
	// Rule 3b requires EXACT title equality; a merely similar title on a
	// different date is not enough evidence (rule 3 handles same-date)
	cands := []downloadCandidate{
		{podcastTitle: "P", published: "2023-06-15T16:00:00Z",
			title:       "Interview with Jane Smith",
			episodeHash: "h1", guid: "g1", have: true},
		{podcastTitle: "P", published: "2024-02-28T17:00:00Z",
			title:       "An Interview with Jane Smith",
			episodeHash: "h2", guid: "g2", have: false},
	}
	if skips := planDownloadSkips(cands); len(skips) != 0 {
		t.Errorf("expected no skips for similar title on different date, got %+v", skips)
	}
}

func TestPlanDownloadSkipsNearDateReissue(t *testing.T) {
	// The 2026-07-20 incident: BBC re-served the Radio 4 edition of a More
	// or Less episode whose World Service edition, dated three days later,
	// was already on disk. Fresh guid, shifted date, prefixed title — only
	// the near-date rule (3a) can see it.
	cands := []downloadCandidate{
		{podcastTitle: "More or Less", published: "2015-12-08T00:00:00Z",
			title:       "WS MoreOrLess: Climate Change",
			episodeHash: "ws", guid: "urn:bbc:podcast:p03b1kn2", have: true},
		{podcastTitle: "More or Less", published: "2015-12-05T00:00:00Z",
			title:       "Climate Change",
			episodeHash: "r4", guid: "urn:bbc:podcast:p03999ky", have: false},
	}
	skips := planDownloadSkips(cands)
	if s, ok := skips["r4"]; !ok || s.matchedHash != "ws" {
		t.Errorf("expected near-date re-issue to skip against WS edition, got %+v", skips)
	}
}

func TestPlanDownloadSkipsNearDateCaseOnlyRetitle(t *testing.T) {
	// Same batch: identical title up to casing, eight days apart — rule 3b
	// needs the exact raw title, rule 3a must catch the case variant
	cands := []downloadCandidate{
		{podcastTitle: "More or Less", published: "2018-07-23T00:00:00Z",
			title:       "Should we have smaller families to save the planet?",
			episodeHash: "ws", guid: "g-ws", have: true},
		{podcastTitle: "More or Less", published: "2018-07-15T00:00:00Z",
			title:       "Should We Have Smaller Families To Save The Planet?",
			episodeHash: "r4", guid: "g-r4", have: false},
	}
	skips := planDownloadSkips(cands)
	if s, ok := skips["r4"]; !ok || s.matchedHash != "ws" {
		t.Errorf("expected case-only retitle to skip across dates, got %+v", skips)
	}
}

func TestPlanDownloadSkipsNearDateNeedsStrictEvidence(t *testing.T) {
	// The word-set overlap that suffices on the SAME date is not enough
	// evidence across dates: a weekly feed revisiting a topic days later
	// must still download
	cands := []downloadCandidate{
		{podcastTitle: "P", published: "2026-05-01T00:00:00Z",
			title:       "The Magic of Thinking Big | XPO CEO Mario Harik",
			episodeHash: "h1", guid: "g1", have: true},
		{podcastTitle: "P", published: "2026-05-04T00:00:00Z",
			title:       "The Magic of Thinking Big: Mario Harik",
			episodeHash: "h2", guid: "g2", have: false},
	}
	if skips := planDownloadSkips(cands); len(skips) != 0 {
		t.Errorf("expected no cross-date skip on word-set evidence alone, got %+v", skips)
	}
}

func TestPlanDownloadSkipsNearDateWindowBounded(t *testing.T) {
	// Outside the ±10-day window even a case-variant title is not matched
	// (rule 3b still handles exact raw repeats at any distance)
	cands := []downloadCandidate{
		{podcastTitle: "P", published: "2018-07-01T00:00:00Z",
			title:       "Should we have smaller families to save the planet?",
			episodeHash: "h1", guid: "g1", have: true},
		{podcastTitle: "P", published: "2018-07-15T00:00:00Z",
			title:       "Should We Have Smaller Families To Save The Planet?",
			episodeHash: "h2", guid: "g2", have: false},
	}
	if skips := planDownloadSkips(cands); len(skips) != 0 {
		t.Errorf("expected no skip outside the near-date window, got %+v", skips)
	}
}

func TestPlanDownloadSkipsNearDatePartSiblings(t *testing.T) {
	// Multi-part specials straddle days; the digit guard (including
	// spelled-out numbers) must keep them apart
	cands := []downloadCandidate{
		{podcastTitle: "More or Less", published: "2014-12-20T00:00:00Z",
			title:       "WS MoreOrLess: Numbers of the Year part 1.",
			episodeHash: "p1", guid: "g1", have: true},
		{podcastTitle: "More or Less", published: "2014-12-27T00:00:00Z",
			title:       "WS MoreOrLess: Numbers of the Year part 2.",
			episodeHash: "p2", guid: "g2", have: false},
	}
	if skips := planDownloadSkips(cands); len(skips) != 0 {
		t.Errorf("expected no skip for near-date part siblings, got %+v", skips)
	}
}

func TestPlanDownloadSkipsNearDatePreviewNotMatched(t *testing.T) {
	// A downloaded preview must not suppress the full episode released a
	// few days later
	cands := []downloadCandidate{
		{podcastTitle: "P", published: "2026-05-01T00:00:00Z",
			title:       "Preview: How the Fed Broke Money",
			episodeHash: "pre", guid: "g1", have: true},
		{podcastTitle: "P", published: "2026-05-04T00:00:00Z",
			title:       "How the Fed Broke Money",
			episodeHash: "full", guid: "g2", have: false},
	}
	if skips := planDownloadSkips(cands); len(skips) != 0 {
		t.Errorf("expected preview not to suppress the full episode, got %+v", skips)
	}
}

func TestPlanDownloadSkipsIdenticalTitleOtherPodcastNotSkippedByRepeatRule(t *testing.T) {
	// Rule 3b is same-podcast only: an identically-titled episode of a
	// different podcast (no guid link) must not suppress the download
	cands := []downloadCandidate{
		{podcastTitle: "Show A", published: "2023-06-15T16:00:00Z",
			title:       "Christmas Special",
			episodeHash: "h1", guid: "g1", have: true},
		{podcastTitle: "Show B", published: "2024-12-24T17:00:00Z",
			title:       "Christmas Special",
			episodeHash: "h2", guid: "g2", have: false},
	}
	if skips := planDownloadSkips(cands); len(skips) != 0 {
		t.Errorf("expected no cross-podcast repeat skips, got %+v", skips)
	}
}
