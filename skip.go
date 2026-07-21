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
// changed, an exact-title match against a downloaded sibling, any date. The
// digit-sequence guard keeps "Part 1" / "Part 2" siblings distinct, and the
// overlap threshold is relative to title length because a short shared word
// ("markets", "tariffs") is normal between two genuinely different same-day
// episodes of a daily feed.
//
// Cross-date re-issues need one more rule (2026-07-20: BBC re-served Radio 4
// editions of nine More or Less episodes whose World Service editions —
// "WS MoreOrLess: Climate Change" and the like, dated 1-8 days later — were
// already on disk; the same-date rule couldn't see them and the exact-title
// repeat rule didn't match the prefixed titles). Rule 3a matches a
// same-podcast downloaded sibling published within a few days, but only on
// STRICT title evidence — normalized equality, long containment, or equality
// after stripping a short "Label: " prefix — never the word-set path, which
// across dates would let a daily feed's recurring topic titles collide.
//
// planDownloadSkips is pure (no db, no filesystem) so the tricky cases are
// unit-testable, mirroring planDedup. Skips are recorded in the
// skipped_episodes table by the caller for auditing.

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
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
	// Rule 3a window: a downloaded same-podcast sibling published within
	// this many days with a strictly equivalent title is a re-issue. The
	// largest offset observed in the 2026-07-20 batch was 8 days.
	nearDateSkipWindowDays = 10
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

// Standalone spelled-out numbers normalize to digits so "Part One" equals
// "Part 1" — and, just as important, so "Part One" vs "Part Two" is caught
// by the digit-sequence guard instead of slipping past it digit-free.
var numberWordDigits = map[string]string{
	"one": "1", "two": "2", "three": "3", "four": "4", "five": "5",
	"six": "6", "seven": "7", "eight": "8", "nine": "9", "ten": "10",
	"eleven": "11", "twelve": "12",
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
// cosmetic retitle churn ("Ai Goes  Parabolic") doesn't break the overlap,
// and maps spelled-out numbers to digits.
func normalizeTitleForOverlap(s string) string {
	fields := strings.Fields(nonAlnumRe.ReplaceAllString(strings.ToLower(s), " "))
	for i, w := range fields {
		if d, ok := numberWordDigits[w]; ok {
			fields[i] = d
		}
	}
	return strings.Join(fields, " ")
}

// titlePrefixRe matches a short leading "Label: " — the shape broadcasters
// use for edition prefixes ("WS MoreOrLess: Climate Change"). Bounded so a
// colon in the middle of a long sentence-title doesn't count as a label.
var titlePrefixRe = regexp.MustCompile(`^[^:]{1,40}:\s*`)

// Prefixes that mark genuinely different content, not another edition of the
// same episode: a "Preview: X" on disk must never suppress the later full
// "X".
var nonEditionPrefixRe = regexp.MustCompile(`(?i)\b(preview|teaser|trailer|excerpt|clip|sneak)\b`)

// stripTitlePrefix removes one leading "Label: " from a raw title, unless
// stripping would leave nothing.
func stripTitlePrefix(s string) string {
	m := titlePrefixRe.FindString(s)
	if m == "" {
		return s
	}
	if stripped := s[len(m):]; stripped != "" {
		return stripped
	}
	return s
}

// editionMarkerMismatch reports that exactly one of the two titles carries a
// preview/teaser marker — "Preview: X" is different content from "X", even
// though every overlap path (containment included) would otherwise pair
// them.
func editionMarkerMismatch(a, b string) bool {
	return nonEditionPrefixRe.MatchString(a) != nonEditionPrefixRe.MatchString(b)
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

// strictTitleEquivalent reports whether two raw titles are the same episode's
// title under only cosmetic transformation: normalized equality, a long
// containment (truncation or edition-suffix retitle), or equality once a
// short "Label: " prefix is stripped from either side ("WS MoreOrLess:
// Climate Change" vs "Climate Change"). Digit sequences of the full
// normalized titles must match, so "Part 1: X" vs "Part 2: X" stays
// distinct. This is the evidence bar for cross-date matching (rule 3a),
// deliberately tighter than materialTitleOverlap: no word-set path.
func strictTitleEquivalent(a, b string) bool {
	if editionMarkerMismatch(a, b) {
		return false
	}
	na, nb := normalizeTitleForOverlap(a), normalizeTitleForOverlap(b)
	if na == "" || nb == "" || digitSeq(na) != digitSeq(nb) {
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
	nsa := normalizeTitleForOverlap(stripTitlePrefix(a))
	nsb := normalizeTitleForOverlap(stripTitlePrefix(b))
	if nsa == "" || nsb == "" {
		return false
	}
	return nsa == nb || na == nsb || nsa == nsb
}

// materialTitleOverlap reports whether two raw titles overlap enough to call
// them the same episode. Digit sequences must match exactly (Part 1 vs Part 2
// are different episodes). Then a strict equivalence (equality, containment,
// prefix-strip) decides; anything else — reorderings like "Ai Goes Parabolic
// | Greg Brockman, Co-Founder OpenAI" vs "... | OpenAI Co-Founder Greg
// Brockman" — must share most of its content words. Substring length is
// deliberately NOT the criterion: series episodes share long title templates
// ("The Rise and Fall of ...") while being different episodes.
func materialTitleOverlap(a, b string) bool {
	if editionMarkerMismatch(a, b) {
		return false
	}
	if strictTitleEquivalent(a, b) {
		return true
	}
	na, nb := normalizeTitleForOverlap(a), normalizeTitleForOverlap(b)
	if na == "" || nb == "" || digitSeq(na) != digitSeq(nb) {
		return false
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

func parseDate10(s string) (time.Time, bool) {
	t, err := time.Parse("2006-01-02", publishedDate10(s))
	return t, err == nil
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
	// Same-podcast index of already-downloaded rows, any date, for the
	// near-date re-issue rule (3a).
	haveByPod := make(map[string][]downloadCandidate)
	// Same-podcast index by exact raw title, any date, for repeats whose
	// guid AND published date both changed (see rule 3b).
	haveByPodTitle := make(map[string][]downloadCandidate)
	titleKey := func(c downloadCandidate) string {
		return c.podcastTitle + "\x00" + c.title
	}
	for _, c := range cands {
		if c.have {
			haveByPodDate[dateKey(c)] = append(haveByPodDate[dateKey(c)], c)
			haveByPod[c.podcastTitle] = append(haveByPod[c.podcastTitle], c)
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

		// Rule 3a: a downloaded same-podcast episode published within a few
		// days whose title is strictly equivalent — a re-issue of an episode
		// held under another edition's title and date (2026-07-20: BBC served
		// Radio 4-titled More or Less episodes whose World Service editions,
		// dated 1-8 days later, were already on disk; rule 3 needs an exact
		// date match and rule 3b an exact raw title, so all of them slipped
		// through). Across dates the word-set path would let a daily feed's
		// recurring topic titles collide, so only strictTitleEquivalent
		// counts. Prefer the nearest date.
		if cd, ok := parseDate10(c.published); ok {
			var nearSib *downloadCandidate
			nearDelta := 0
			for i := range haveByPod[c.podcastTitle] {
				sib := haveByPod[c.podcastTitle][i]
				if sib.episodeHash == c.episodeHash {
					continue
				}
				sd, ok := parseDate10(sib.published)
				if !ok {
					continue
				}
				delta := int(cd.Sub(sd).Hours() / 24)
				if delta < 0 {
					delta = -delta
				}
				if delta > nearDateSkipWindowDays || !strictTitleEquivalent(c.title, sib.title) {
					continue
				}
				if nearSib == nil || delta < nearDelta ||
					(delta == nearDelta && sib.episodeHash < nearSib.episodeHash) {
					nearSib = &haveByPod[c.podcastTitle][i]
					nearDelta = delta
				}
			}
			if nearSib != nil {
				skips[c.episodeHash] = downloadSkip{
					matchedHash:  nearSib.episodeHash,
					matchedTitle: nearSib.title,
					reason: fmt.Sprintf("retitle: published %dd apart with equivalent title to downloaded episode %s",
						nearDelta, nearSib.episodeHash),
				}
				continue
			}
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
