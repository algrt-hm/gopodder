package main

// skip.go -- pre-download retitle detection.
//
// Some podcasts (The Knowledge Project is a chronic case) republish the same
// episode under a new title, sometimes repeatedly. Each retitle gets a new
// podcastname_episodename_hash, so the episode looks brand-new to the
// download logic and gets fetched again: the 2026-07-05 run added a fourth
// ~250MB copy of several episodes that were already on disk three times.
//
// The feed GUID is the episode's stable identity: every row in this db has
// one, and it survives every observed retitle (including the ones that also
// shift the published date, which a date-based check would miss). So the
// primary rule is: if another episodes row with the same (podcast, guid)
// already has a file, this row is a retitle of it — skip.
//
// The guid also survives a rename of the whole podcast (2026-07-09: BBC
// retitled "Arts & Ideas" to "Free Thinking", re-hashing 1,486 episodes), so
// a downloaded same-guid episode of a DIFFERENT podcast also marks a row as
// a duplicate — but only with a corroborating title, since junk guids ("1",
// a date) can collide between unrelated feeds.
//
// The fallback, for feeds that rotate GUIDs, is the title heuristic: same
// podcast, same published date, digit sequences equal, and a material common
// substring between the titles; plus, for repeats whose guid and date both
// changed, an exact-title match against a downloaded sibling, any date. The digit-sequence guard keeps "Part 1" /
// "Part 2" siblings distinct, and the overlap threshold is relative to title
// length because a short shared word ("markets", "tariffs") is normal between
// two genuinely different same-day episodes of a daily feed.
//
// planDownloadSkips is pure (no db, no filesystem) so the tricky cases are
// unit-testable, mirroring planDedup. Skips are recorded in the
// skipped_episodes table by the caller for auditing.

import (
	"regexp"
	"sort"
	"strings"
)

const (
	// Containment path: a normalized title fully contained in the other is a
	// truncation retitle, but only when it is long enough that containment
	// can't happen by accident between distinct episodes.
	retitleContainMinRunes = 15
	// Word-set path: Jaccard similarity of the stopword-filtered word sets,
	// with a minimum number of shared content words. Distinct episodes of a
	// series share a title template ("The Rise and Fall of ...") but their
	// content words diverge; retitles keep the content words (guest, topic)
	// and shuffle everything else.
	retitleJaccardMin   = 0.65
	retitleMinWordsHits = 3
)

// Filtered out before word-set comparison: template glue that inflates
// similarity between genuinely different titles. "s" is what possessives
// normalize to.
var retitleStopwords = map[string]bool{
	"a": true, "an": true, "and": true, "as": true, "at": true, "by": true,
	"for": true, "from": true, "in": true, "is": true, "it": true, "of": true,
	"on": true, "or": true, "s": true, "the": true, "to": true, "vs": true,
	"with": true,
}

// downloadCandidate is one episodes row considered for download. have means a
// copy already exists under this row's own identity (on a scan path, in the
// archive registry, or legacy-named).
type downloadCandidate struct {
	podcastTitle string
	published    string // IFNULL(published, first_seen)
	title        string
	episodeHash  string
	guid         string
	firstSeen    string
	lastSeen     string
	have         bool
}

// downloadSkip says why a candidate should not be downloaded and which
// sibling episode it duplicates.
type downloadSkip struct {
	matchedHash  string
	matchedTitle string
	reason       string
}

var nonAlnumRe = regexp.MustCompile(`[^a-z0-9]+`)

// normalizeTitleForOverlap lowercases and collapses punctuation/whitespace so
// cosmetic retitle churn ("Ai Goes  Parabolic") doesn't break the overlap.
func normalizeTitleForOverlap(s string) string {
	return strings.TrimSpace(nonAlnumRe.ReplaceAllString(strings.ToLower(s), " "))
}

// longestCommonSubstringLen returns the length in runes of the longest common
// substring of a and b (classic DP, rolling rows).
func longestCommonSubstringLen(a, b string) int {
	ra, rb := []rune(a), []rune(b)
	if len(ra) == 0 || len(rb) == 0 {
		return 0
	}
	prev := make([]int, len(rb)+1)
	cur := make([]int, len(rb)+1)
	best := 0
	for i := 1; i <= len(ra); i++ {
		for j := 1; j <= len(rb); j++ {
			if ra[i-1] == rb[j-1] {
				cur[j] = prev[j-1] + 1
				if cur[j] > best {
					best = cur[j]
				}
			} else {
				cur[j] = 0
			}
		}
		prev, cur = cur, prev
		for j := range cur {
			cur[j] = 0
		}
	}
	return best
}

// materialTitleOverlap reports whether two raw titles overlap enough to call
// them the same episode. Digit sequences must match exactly (Part 1 vs Part 2
// are different episodes). Then a truncation retitle is one normalized title
// contained in the other; anything else — reorderings like "Ai Goes Parabolic
// | Greg Brockman, Co-Founder OpenAI" vs "... | OpenAI Co-Founder Greg
// Brockman" — must share most of its content words. Substring length is
// deliberately NOT the criterion: series episodes share long title templates
// ("The Rise and Fall of ...") while being different episodes.
func materialTitleOverlap(a, b string) bool {
	if digitSeq(a) != digitSeq(b) {
		return false
	}
	na, nb := normalizeTitleForOverlap(a), normalizeTitleForOverlap(b)
	if na == "" || nb == "" {
		return false
	}
	if na == nb {
		return true
	}

	shorter, longer := na, nb
	if len([]rune(nb)) < len([]rune(na)) {
		shorter, longer = nb, na
	}
	if len([]rune(shorter)) >= retitleContainMinRunes && strings.Contains(longer, shorter) {
		return true
	}

	wa, wb := contentWordSet(na), contentWordSet(nb)
	inter := 0
	for w := range wa {
		if wb[w] {
			inter++
		}
	}
	union := len(wa) + len(wb) - inter
	return union > 0 && inter >= retitleMinWordsHits &&
		float64(inter)/float64(union) >= retitleJaccardMin
}

func contentWordSet(normalized string) map[string]bool {
	out := make(map[string]bool)
	for _, w := range strings.Fields(normalized) {
		if !retitleStopwords[w] {
			out[w] = true
		}
	}
	return out
}

func publishedDate10(s string) string {
	if len(s) >= 10 {
		return s[:10]
	}
	return s
}

// newerCandidate reports whether a is the fresher row: later last_seen, then
// later first_seen, then smaller hash for determinism.
func newerCandidate(a, b downloadCandidate) bool {
	if a.lastSeen != b.lastSeen {
		return a.lastSeen > b.lastSeen
	}
	if a.firstSeen != b.firstSeen {
		return a.firstSeen > b.firstSeen
	}
	return a.episodeHash < b.episodeHash
}

// planDownloadSkips decides, for every candidate not yet downloaded, whether
// it is a retitle of an episode we already have (or of a fresher pending row)
// and should be skipped. Returns skip decisions keyed by episode hash.
func planDownloadSkips(cands []downloadCandidate) map[string]downloadSkip {
	skips := make(map[string]downloadSkip)

	// Group rows by (podcast, guid); empty guids get no guid-based handling.
	guidKey := func(c downloadCandidate) string {
		return c.podcastTitle + "\x00" + c.guid
	}
	guidGroups := make(map[string][]downloadCandidate)
	for _, c := range cands {
		if c.guid != "" {
			guidGroups[guidKey(c)] = append(guidGroups[guidKey(c)], c)
		}
	}

	// Index of already-downloaded rows by (podcast, published date) for the
	// title fallback.
	haveByPodDate := make(map[string][]downloadCandidate)
	dateKey := func(c downloadCandidate) string {
		return c.podcastTitle + "\x00" + publishedDate10(c.published)
	}
	// Cross-podcast index of already-downloaded rows by guid alone, for
	// whole-podcast renames (2026-07-09: BBC retitled "Arts & Ideas" to
	// "Free Thinking" and 1,486 episodes re-hashed as new under the new
	// podcast title, invisible to every same-podcast rule).
	haveByGuid := make(map[string][]downloadCandidate)
	// Same-podcast index by exact raw title, any date, for repeats whose
	// guid AND published date both changed (see rule 3b).
	haveByPodTitle := make(map[string][]downloadCandidate)
	titleKey := func(c downloadCandidate) string {
		return c.podcastTitle + "\x00" + c.title
	}
	for _, c := range cands {
		if c.have {
			haveByPodDate[dateKey(c)] = append(haveByPodDate[dateKey(c)], c)
			haveByPodTitle[titleKey(c)] = append(haveByPodTitle[titleKey(c)], c)
			if c.guid != "" {
				haveByGuid[c.guid] = append(haveByGuid[c.guid], c)
			}
		}
	}

	for _, c := range cands {
		if c.have {
			continue
		}

		// Rule 1: a same-guid sibling already has a file — this row is a
		// retitle of it. Prefer the freshest such sibling for the record.
		if c.guid != "" {
			var haveSib *downloadCandidate
			pendingWinner := c
			for i := range guidGroups[guidKey(c)] {
				sib := guidGroups[guidKey(c)][i]
				if sib.episodeHash == c.episodeHash {
					continue
				}
				if sib.have {
					if haveSib == nil || newerCandidate(sib, *haveSib) {
						haveSib = &guidGroups[guidKey(c)][i]
					}
				} else if newerCandidate(sib, pendingWinner) {
					pendingWinner = sib
				}
			}
			if haveSib != nil {
				skips[c.episodeHash] = downloadSkip{
					matchedHash:  haveSib.episodeHash,
					matchedTitle: haveSib.title,
					reason:       "retitle: same guid as downloaded episode " + haveSib.episodeHash,
				}
				continue
			}
			// Rule 1b: an already-downloaded episode of ANOTHER podcast has
			// this guid — the podcast itself was renamed, so the copy on disk
			// lives under the old show name. Guid alone is not trustworthy
			// across podcasts (feeds that use junk guids like "1" or a date
			// can collide between unrelated shows), so the episode title must
			// corroborate.
			var renameSib *downloadCandidate
			for i := range haveByGuid[c.guid] {
				sib := haveByGuid[c.guid][i]
				if sib.podcastTitle == c.podcastTitle || sib.episodeHash == c.episodeHash {
					continue
				}
				if !materialTitleOverlap(c.title, sib.title) {
					continue
				}
				if renameSib == nil || newerCandidate(sib, *renameSib) {
					renameSib = &haveByGuid[c.guid][i]
				}
			}
			if renameSib != nil {
				skips[c.episodeHash] = downloadSkip{
					matchedHash:  renameSib.episodeHash,
					matchedTitle: renameSib.title,
					reason: "rename: same guid and title as downloaded episode " +
						renameSib.episodeHash + " of podcast " + renameSib.podcastTitle,
				}
				continue
			}
			// Rule 2: several pending rows share the guid (retitled before we
			// ever downloaded it) — only the freshest row, i.e. the title the
			// feed currently uses, gets downloaded.
			if pendingWinner.episodeHash != c.episodeHash {
				skips[c.episodeHash] = downloadSkip{
					matchedHash:  pendingWinner.episodeHash,
					matchedTitle: pendingWinner.title,
					reason:       "retitle: superseded by fresher pending row " + pendingWinner.episodeHash,
				}
				continue
			}
		}

		// Rule 3 (fallback for guid-rotating feeds): an already-downloaded
		// episode of the same podcast on the same published date whose title
		// overlaps materially. Pick the strongest overlap.
		matches := make([]downloadCandidate, 0, 1)
		for _, h := range haveByPodDate[dateKey(c)] {
			if h.episodeHash != c.episodeHash && materialTitleOverlap(c.title, h.title) {
				matches = append(matches, h)
			}
		}
		if len(matches) > 0 {
			sort.Slice(matches, func(i, j int) bool {
				li := longestCommonSubstringLen(normalizeTitleForOverlap(c.title), normalizeTitleForOverlap(matches[i].title))
				lj := longestCommonSubstringLen(normalizeTitleForOverlap(c.title), normalizeTitleForOverlap(matches[j].title))
				if li != lj {
					return li > lj
				}
				return matches[i].episodeHash < matches[j].episodeHash
			})
			skips[c.episodeHash] = downloadSkip{
				matchedHash:  matches[0].episodeHash,
				matchedTitle: matches[0].title,
				reason:       "retitle: same date and overlapping title as downloaded episode " + matches[0].episodeHash,
			}
			continue
		}

		// Rule 3b: an already-downloaded same-podcast episode with the
		// IDENTICAL raw title, any date — a repeat re-entering the feed with
		// a fresh guid and its re-broadcast date. This was invisible before
		// podcast renames existed: the episode hash is md5(podcast + title),
		// so a same-titled item collapsed into the existing row at ingestion
		// and never became a download candidate. Exact equality (not
		// materialTitleOverlap) keeps this as tight as the old hash identity.
		var repeatSib *downloadCandidate
		for i := range haveByPodTitle[titleKey(c)] {
			sib := haveByPodTitle[titleKey(c)][i]
			if sib.episodeHash == c.episodeHash {
				continue
			}
			if repeatSib == nil || newerCandidate(sib, *repeatSib) {
				repeatSib = &haveByPodTitle[titleKey(c)][i]
			}
		}
		if repeatSib != nil {
			skips[c.episodeHash] = downloadSkip{
				matchedHash:  repeatSib.episodeHash,
				matchedTitle: repeatSib.title,
				reason:       "repeat: identical title as downloaded episode " + repeatSib.episodeHash,
			}
		}
	}
	return skips
}
