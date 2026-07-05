package main

// dedup.go -- remove duplicate podcast files that differ only in their
// filename hash ("twins"), replacing dedup_legacy_twins.sh.
//
// Background: gopodder filenames end in a hash. Older versions used the md5
// of the file URL; newer versions use the md5 of podcast title + episode
// title. Feeds with rotating auth tokens (Patreon) produced several
// URL-hash copies of the same episode over the years, and the March and
// July 2026 re-download incidents added episode-hash twins (and ~150-byte
// wget error stubs) next to them.
//
// Unlike the shell script, this has the database, so every copy is
// attributed to an episode: directly when its filename hash is a known
// episode hash, or via file_url_hash for legacy names. Distinct episodes
// whose titles differ only in digits ("Part 1"/"Part 2" published the same
// day) collapse to the same filename prefix because titleTransformation
// strips digits; attribution plus row liveness (last_seen refreshed by feed
// parses) and digit-sequence comparison tells genuine distinct episodes
// apart from retitled duplicates, which filenames alone cannot.
//
// Invariant: a file is only ever deleted when a surviving copy of the SAME
// episode is kept; the keeper ends up under the canonical episode-hash
// filename so detection recognises it directly. Anything the evidence
// doesn't decide is reported as MANUAL and left alone.
//
// Dry run by default (--dedup-twins); --dedup-twins-delete applies the plan
// and maintains the downloads and archived_episodes tables in the same
// transaction, so no separate SQL file or reconcile pass is needed.

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	dedupStubMaxBytes      = 4096
	dedupBigKeeperMinBytes = 1000000
	dedupSizeSlackBytes    = 2000000
	// A row the feed parser has refreshed within this window belongs to an
	// episode still listed in a feed; older rows are leftovers from retitles
	// or feed removals.
	dedupLivenessWindow = 7 * 24 * time.Hour
)

type dedupOwner struct {
	epHash       string
	podcastTitle string
	title        string
	published    string // IFNULL(published, first_seen), never empty
	lastSeen     string
	interactive  bool
}

type dedupFile struct {
	path    string
	dir     string
	name    string
	hash    string
	prefix  string
	size    int64
	epHash  string // resolved owner episode hash, or ""
	pruneEp string // stale episodes row to prune if this file is removed/renamed
}

// dedupAction kinds
const (
	actDelete   = "delete"   // redundant copy, keeper survives
	actStub     = "stub"     // failed-download stub next to a real keeper
	actRename   = "rename"   // keeper moves to its canonical filename
	actSkip     = "skip"     // keeper suspiciously small vs this copy
	actSameName = "samename" // same filename in two dirs; not touched
	actManual   = "manual"   // evidence inconclusive; not touched
)

type dedupAction struct {
	kind       string
	file       dedupFile
	keeperPath string
	newPath    string // rename target (actRename)
	reason     string
	pruneEp    string // stale episodes row to prune alongside this action
}

// loadDedupOwners returns episode attribution data from both episode tables:
// hash -> owner, and file_url_hash -> episode hash for legacy names.
func loadDedupOwners(dbFile string) (map[string]dedupOwner, map[string]string, error) {
	byHash := make(map[string]dedupOwner)
	url2ep := make(map[string]string)

	db, err := sql.Open(sqlite3, dbFile)
	if err != nil {
		return nil, nil, err
	}
	defer db.Close()

	load := func(table string, interactive bool) error {
		q := fmt.Sprintf(`SELECT podcastname_episodename_hash, podcast_title, title,
			IFNULL(published, first_seen), last_seen, IFNULL(file_url_hash, '')
			FROM %s;`, table)
		rows, err := db.Query(q)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var o dedupOwner
			var urlHash string
			if err := rows.Scan(&o.epHash, &o.podcastTitle, &o.title, &o.published, &o.lastSeen, &urlHash); err != nil {
				return err
			}
			o.interactive = interactive
			// episodes (feed-refreshed) rows take precedence over interactive ones
			if existing, ok := byHash[o.epHash]; !ok || (existing.interactive && !interactive) {
				byHash[o.epHash] = o
			}
			if urlHash != "" {
				url2ep[urlHash] = o.epHash
			}
		}
		return rows.Err()
	}

	if err := load("episodes", false); err != nil {
		return nil, nil, err
	}
	if err := load("interactive_episodes", true); err != nil {
		return nil, nil, err
	}
	return byHash, url2ep, nil
}

// gatherDedupFiles lists podcast files across the scan paths with sizes.
func gatherDedupFiles(scanPaths []string) ([]dedupFile, error) {
	out := make([]dedupFile, 0)
	for _, dir := range scanPaths {
		names, err := archiveCandidatesInDir(dir)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", dir, err)
		}
		for _, name := range names {
			hash, _, err := hashFromFilename(name)
			if err != nil || len(hash) != 32 {
				continue
			}
			prefix, ok := nameMinusHash(name)
			if !ok {
				continue
			}
			info, err := os.Stat(filepath.Join(dir, name))
			if err != nil {
				continue
			}
			out = append(out, dedupFile{
				path:   filepath.Join(dir, name),
				dir:    dir,
				name:   name,
				hash:   hash,
				prefix: prefix,
				size:   info.Size(),
			})
		}
	}
	return out, nil
}

func ownerIsLive(o dedupOwner, now time.Time) bool {
	if o.interactive {
		// interactive rows are never feed-refreshed; treat as live
		return true
	}
	t, err := time.Parse(time.RFC3339, o.lastSeen)
	if err != nil {
		return true // unparseable: err on the safe side
	}
	return now.Sub(t) <= dedupLivenessWindow
}

var digitRunRe = regexp.MustCompile(`[0-9]+`)

// digitSeq concatenates the digit runs of a title. titleTransformation strips
// digits from filenames, so this is the part of two colliding titles that can
// still distinguish "Part 1" from "Part 2".
func digitSeq(s string) string {
	return strings.Join(digitRunRe.FindAllString(s, -1), ",")
}

// Episode-number tokens: a leading "#199 ", "#199: ", "/391/ ", or a trailing
// "| #199", "(2019)". Both require an explicit marker character, so a bare
// trailing "Part 2" or leading "2 Fast 2 Furious" is never treated as an
// episode number.
var (
	leadingEpNumRe  = regexp.MustCompile(`^\s*(?:#\s*\d+|/\d+/)\s*[:\-\.\)/]*\s*`)
	trailingEpNumRe = regexp.MustCompile(`\s*[|#(\[][\s#]*\d+[)\]]?\s*$`)
)

// sameEpisodeTitles reports whether two feed titles describe the same episode
// once shifting episode numbering is removed: feeds drop or renumber "#199 "
// prefixes and "| #199" suffixes over time, which changes the digit sequence
// without changing the episode. Titles whose digits differ in the remainder
// ("Part 1" vs "Part 2") stay distinct.
//
// BOTH the digit sequence AND the transformed text must match. An earlier
// version returned true on digit equality alone — vacuously true for any two
// digit-free titles — which let the retitle pass pair unrelated daily feeds
// and merge same-day episodes with entirely different titles (the 2026-07-05
// FT News Briefing incident). The twin pass was shielded only by its
// shared-prefix grouping; never rely on that here.
func sameEpisodeTitles(a, b string) bool {
	if digitSeq(a) == digitSeq(b) && titleTransformation(a) == titleTransformation(b) {
		return true
	}
	sa := trailingEpNumRe.ReplaceAllString(leadingEpNumRe.ReplaceAllString(a, ""), "")
	sb := trailingEpNumRe.ReplaceAllString(leadingEpNumRe.ReplaceAllString(b, ""), "")
	return digitSeq(sa) == digitSeq(sb) && titleTransformation(sa) == titleTransformation(sb)
}

// canonicalNameFor returns the canonical filename for an owner, or "" if the
// row can't produce one.
func canonicalNameFor(o dedupOwner) string {
	if len([]rune(o.published)) < 10 {
		return ""
	}
	return buildNonInteractiveFilename(o.podcastTitle, o.title, o.published, o.epHash)
}

// chooseKeeper picks the index of the copy to keep: the largest copy already
// named with canonicalHash, unless it is a stub or truncated relative to the
// largest copy overall; else the largest copy overall.
func chooseKeeper(files []dedupFile, canonicalHash string) int {
	k := -1
	if canonicalHash != "" {
		for i, f := range files {
			if f.hash == canonicalHash && (k == -1 || f.size > files[k].size) {
				k = i
			}
		}
	}
	m := 0
	for i, f := range files {
		if f.size > files[m].size {
			m = i
		}
	}
	if k == -1 {
		return m
	}
	if files[k].size < files[m].size &&
		((files[k].size < dedupStubMaxBytes && files[m].size > dedupBigKeeperMinBytes) ||
			(files[k].size < files[m].size-dedupSizeSlackBytes && files[k].size*100 < files[m].size*95)) {
		return m
	}
	return k
}

// dedupCopies emits actions reducing files (all copies of one episode) to a
// single keeper, renaming the keeper to canonicalName when it is known and
// the keeper is not already named with the episode hash.
func dedupCopies(files []dedupFile, epHash, canonicalName string, actions *[]dedupAction) {
	if len(files) == 0 {
		return
	}
	k := chooseKeeper(files, epHash)
	keeper := files[k]
	for i, f := range files {
		if i == k {
			continue
		}
		switch {
		case f.name == keeper.name:
			*actions = append(*actions, dedupAction{kind: actSameName, file: f, keeperPath: keeper.path})
		case f.size < dedupStubMaxBytes && keeper.size > dedupBigKeeperMinBytes:
			*actions = append(*actions, dedupAction{kind: actStub, file: f, keeperPath: keeper.path, pruneEp: f.pruneEp})
		case keeper.size < f.size-dedupSizeSlackBytes && keeper.size*100 < f.size*95:
			*actions = append(*actions, dedupAction{kind: actSkip, file: f, keeperPath: keeper.path,
				reason: fmt.Sprintf("keeper %d bytes vs %d", keeper.size, f.size)})
		default:
			*actions = append(*actions, dedupAction{kind: actDelete, file: f, keeperPath: keeper.path, pruneEp: f.pruneEp})
		}
	}
	if epHash != "" && keeper.hash != epHash && canonicalName != "" && canonicalName != keeper.name {
		*actions = append(*actions, dedupAction{kind: actRename, file: keeper,
			newPath: filepath.Join(keeper.dir, canonicalName), pruneEp: keeper.pruneEp})
	}
}

// planDedup builds the action list for all twin groups. Pure: no filesystem
// or database access, so the tricky cases are unit-testable.
func planDedup(files []dedupFile, owners map[string]dedupOwner, url2ep map[string]string, now time.Time) []dedupAction {
	// Attribute every file to an episode where the db allows
	groups := make(map[string][]dedupFile)
	for _, f := range files {
		if _, ok := owners[f.hash]; ok {
			f.epHash = f.hash
		} else if ep, ok := url2ep[f.hash]; ok {
			f.epHash = ep
		}
		groups[f.prefix] = append(groups[f.prefix], f)
	}

	prefixes := make([]string, 0, len(groups))
	for p := range groups {
		prefixes = append(prefixes, p)
	}
	sort.Strings(prefixes)

	actions := make([]dedupAction, 0)
	for _, p := range prefixes {
		group := groups[p]
		if len(group) < 2 {
			continue
		}

		// Distinct owning episodes, split by liveness
		liveEps := make(map[string]dedupOwner)
		staleEps := make(map[string]dedupOwner)
		for _, f := range group {
			if f.epHash == "" {
				continue
			}
			o := owners[f.epHash]
			if ownerIsLive(o, now) {
				liveEps[f.epHash] = o
			} else {
				staleEps[f.epHash] = o
			}
		}

		switch {
		case len(liveEps) <= 1:
			// At most one episode the feeds still know under this prefix.
			// Stale-owned copies are retitle leftovers of that episode iff
			// their titles carry the same digit sequence; a differing digit
			// sequence means a genuinely different episode (a "Part N"
			// sibling that fell out of the feed) — leave those alone.
			var groupEp string
			var canonical string
			for ep, o := range liveEps {
				groupEp = ep
				canonical = canonicalNameFor(o)
			}
			if groupEp == "" && len(staleEps) == 1 {
				for ep, o := range staleEps {
					groupEp = ep
					canonical = canonicalNameFor(o)
				}
			}

			// Note: matched copies are treated as the same audio regardless
			// of size difference — feeds systematically re-encode (the July
			// 2026 data showed a uniform ~1.5x ratio between numbered-title
			// originals and their renumbered live copies), so a size gate
			// here blocks correct merges. chooseKeeper prefers the canonical
			// name but falls back to the largest copy when the canonical one
			// is a stub or truncated, so the better copy survives either way.
			eligible := make([]dedupFile, 0, len(group))
			conflicted := false
			for _, f := range group {
				if f.epHash != "" && groupEp != "" && f.epHash != groupEp {
					stale := owners[f.epHash]
					if !sameEpisodeTitles(stale.title, owners[groupEp].title) {
						actions = append(actions, dedupAction{kind: actManual, file: f,
							reason: fmt.Sprintf("digit-colliding sibling of %s (%q vs %q)",
								groupEp, stale.title, owners[groupEp].title)})
						conflicted = true
						continue
					}
					if !stale.interactive {
						f.pruneEp = f.epHash
					}
				}
				eligible = append(eligible, f)
			}
			// If a sibling episode is present, unattributed copies could
			// belong to either episode; don't guess.
			if conflicted {
				kept := eligible[:0]
				for _, f := range eligible {
					if f.epHash == "" {
						actions = append(actions, dedupAction{kind: actManual, file: f,
							reason: "unattributable copy in a group with digit-colliding siblings"})
					} else {
						kept = append(kept, f)
					}
				}
				eligible = kept
			}
			dedupCopies(eligible, groupEp, canonical, &actions)

		default:
			// Two or more live episodes share the prefix (same-day digit
			// collision): dedup per episode; only attributed copies are safe
			// to touch.
			byEp := make(map[string][]dedupFile)
			for _, f := range group {
				if f.epHash == "" {
					actions = append(actions, dedupAction{kind: actManual, file: f,
						reason: fmt.Sprintf("unattributable copy; %d live episodes share prefix %s", len(liveEps), p)})
					continue
				}
				target := f.epHash
				if _, stale := staleEps[f.epHash]; stale {
					// A stale owner here is a retitle leftover of whichever
					// live sibling its title matches once episode numbering
					// is stripped
					matches := make([]string, 0, 1)
					for ep, o := range liveEps {
						if sameEpisodeTitles(o.title, owners[f.epHash].title) {
							matches = append(matches, ep)
						}
					}
					if len(matches) != 1 {
						actions = append(actions, dedupAction{kind: actManual, file: f,
							reason: fmt.Sprintf("stale copy matches %d of %d live episodes on prefix %s", len(matches), len(liveEps), p)})
						continue
					}
					target = matches[0]
					if !owners[f.epHash].interactive {
						f.pruneEp = f.epHash
					}
				}
				byEp[target] = append(byEp[target], f)
			}
			eps := make([]string, 0, len(byEp))
			for ep := range byEp {
				eps = append(eps, ep)
			}
			sort.Strings(eps)
			for _, ep := range eps {
				dedupCopies(byEp[ep], ep, canonicalNameFor(owners[ep]), &actions)
			}
		}
	}
	return actions
}

// runDedupTwins gathers files across scanPaths, plans, prints, and (when
// apply is true) executes: file deletions/renames plus downloads and
// archived_episodes maintenance in one transaction.
func runDedupTwins(scanPaths []string, apply bool) error {
	owners, url2ep, err := loadDedupOwners(dbFileName)
	if err != nil {
		return err
	}
	files, err := gatherDedupFiles(scanPaths)
	if err != nil {
		return err
	}
	actions := planDedup(files, owners, url2ep, time.Now())
	return executeDedupPlan(actions, scanPaths, apply, "--dedup-twins-delete")
}

// executeDedupPlan prints a plan and, when apply is true, executes it: file
// deletions/renames plus downloads, archived_episodes, and stale episodes-row
// maintenance in one transaction. Shared by the twin and retitle passes.
func executeDedupPlan(actions []dedupAction, scanPaths []string, apply bool, applyFlag string) error {
	archiveDirs := make(map[string]bool)
	for _, d := range scanPaths[1:] {
		archiveDirs[d] = true
	}

	would := "would "
	if apply {
		would = ""
	}

	var tx *sql.Tx
	if apply {
		db, err := sql.Open(sqlite3, dbFileName)
		if err != nil {
			return err
		}
		defer db.Close()
		tx, err = db.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	// A deleted or renamed-away stale variant leaves its episodes row without
	// a file; that row would re-queue the variant for download on the next
	// run (its prefix has two owners, which disables the twin backstop), so
	// prune it in the same transaction.
	pruneRow := func(epHash string) error {
		if !apply || epHash == "" {
			return nil
		}
		_, err := tx.Exec(`DELETE FROM episodes WHERE podcastname_episodename_hash = ?;`, epHash)
		return err
	}

	removeFile := func(f dedupFile) error {
		if !apply {
			return nil
		}
		if err := os.Remove(f.path); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM downloads WHERE filename = ?;`, f.name); err != nil {
			return err
		}
		if archiveDirs[f.dir] {
			if _, err := tx.Exec(`DELETE FROM archived_episodes WHERE podcastname_episodename_hash = ?;`, f.hash); err != nil {
				return err
			}
		}
		return nil
	}

	counts := make(map[string]int)
	var reclaimed int64
	for _, a := range actions {
		switch a.kind {
		case actDelete, actStub:
			label := "duplicate"
			if a.kind == actStub {
				label = "failed-download stub"
			}
			fmt.Printf("%sdelete %s: %s (%d bytes; keeping %s)\n", would, label, a.file.path, a.file.size, a.keeperPath)
			if err := removeFile(a.file); err != nil {
				return err
			}
			if a.pruneEp != "" {
				fmt.Printf("%sprune stale episodes row %s\n", would, a.pruneEp)
				if err := pruneRow(a.pruneEp); err != nil {
					return err
				}
			}
			reclaimed += a.file.size
		case actRename:
			if _, err := os.Stat(a.newPath); err == nil {
				fmt.Printf("NOT renaming (target exists): %s -> %s\n", a.file.path, a.newPath)
				continue
			}
			fmt.Printf("%srename keeper to canonical name: %s -> %s\n", would, a.file.path, a.newPath)
			if a.pruneEp != "" {
				fmt.Printf("%sprune stale episodes row %s\n", would, a.pruneEp)
				if err := pruneRow(a.pruneEp); err != nil {
					return err
				}
			}
			if apply {
				if err := os.Rename(a.file.path, a.newPath); err != nil {
					return err
				}
				if _, err := tx.Exec(`DELETE FROM downloads WHERE filename = ?;`, a.file.name); err != nil {
					return err
				}
				if archiveDirs[a.file.dir] {
					newName := filepath.Base(a.newPath)
					newHash, _, err := hashFromFilename(newName)
					if err == nil {
						if _, err := tx.Exec(`DELETE FROM archived_episodes WHERE podcastname_episodename_hash = ?;`, a.file.hash); err != nil {
							return err
						}
						if _, err := tx.Exec(`
							INSERT INTO archived_episodes (podcastname_episodename_hash, archived_path, archived_at)
							VALUES (?, ?, ?)
							ON CONFLICT(podcastname_episodename_hash) DO UPDATE SET
								archived_path = excluded.archived_path, archived_at = excluded.archived_at;`,
							newHash, a.newPath, ts); err != nil {
							return err
						}
					}
				}
			}
		case actSkip:
			fmt.Printf("SKIP (%s): %s (keeper %s)\n", a.reason, a.file.path, a.keeperPath)
		case actSameName:
			fmt.Printf("NOTE same filename in two dirs (not touching): %s (also %s)\n", a.file.path, a.keeperPath)
		case actManual:
			fmt.Printf("MANUAL (%s): %s\n", a.reason, a.file.path)
		}
		counts[a.kind]++
	}

	if apply {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	fmt.Printf("\n%s%d duplicates and %d stubs (%.1f GiB), %d keeper renames, %d skipped, %d same-name, %d manual\n",
		map[bool]string{true: "Applied: ", false: "Dry run: "}[apply],
		counts[actDelete], counts[actStub], float64(reclaimed)/1073741824.0,
		counts[actRename], counts[actSkip], counts[actSameName], counts[actManual])
	if !apply {
		fmt.Printf("Re-run with %s to apply.\n", applyFlag)
	}
	return nil
}

// pruneStaleEpisodes removes episodes rows that would re-queue deleted twin
// variants for download: rows the feed parser has stopped refreshing, that
// carry a download URL, and whose episode has no surviving copy — not on any
// scan path (by episode hash or by the row's own file_url_hash for
// legacy-named files) and not in the archive registry. Steady state has ~0
// such rows; they appear when dedup deletes a stale variant's file, and each
// one costs a wasted wget (or a fresh error stub) nightly.
func pruneStaleEpisodes(scanPaths []string, apply bool) error {
	files, err := gatherDedupFiles(scanPaths)
	if err != nil {
		return err
	}
	have := make(map[string]bool)
	for _, f := range files {
		have[f.hash] = true
	}
	for _, v := range fetchArchivedHashes().ToSlice() {
		have[fmt.Sprintf("%v", v)] = true
	}

	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		return err
	}
	defer db.Close()

	rows, err := db.Query(`SELECT podcastname_episodename_hash, IFNULL(file_url_hash, ''), podcast_title, title, last_seen
		FROM episodes WHERE file != '' AND file IS NOT NULL;`)
	if err != nil {
		return err
	}

	now := time.Now()
	type staleRow struct{ epHash, podcast, title string }
	toPrune := make([]staleRow, 0)
	for rows.Next() {
		var epHash, urlHash, podcast, title, lastSeen string
		if err := rows.Scan(&epHash, &urlHash, &podcast, &title, &lastSeen); err != nil {
			rows.Close()
			return err
		}
		t, err := time.Parse(time.RFC3339, lastSeen)
		if err != nil || now.Sub(t) <= dedupLivenessWindow {
			continue
		}
		if have[epHash] || (urlHash != "" && have[urlHash]) {
			continue
		}
		toPrune = append(toPrune, staleRow{epHash, podcast, title})
	}
	rows.Close()

	would := "would "
	if apply {
		would = ""
	}
	for _, r := range toPrune {
		fmt.Printf("%sprune stale episodes row %s (%s: %s)\n", would, r.epHash, r.podcast, r.title)
	}

	if apply && len(toPrune) > 0 {
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback()
		for _, r := range toPrune {
			if _, err := tx.Exec(`DELETE FROM episodes WHERE podcastname_episodename_hash = ?;`, r.epHash); err != nil {
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	fmt.Printf("\n%s%d stale fileless episodes row(s)\n",
		map[bool]string{true: "Pruned ", false: "Dry run: would prune "}[apply], len(toPrune))
	if !apply {
		fmt.Println("Re-run with --prune-stale-episodes-delete to apply.")
	}
	return nil
}

// Retitle pass: merge duplicate copies of episodes that a podcast rename
// split across two filename prefixes (e.g. Aufhebunga_Bunga_Patreon-... vs
// Bungacast_Patreon_feed-..., or The_Knowledge_Project_with_Shane_Parrish-...
// back to The_Knowledge_Project-...). The twin pass can't see these: the
// podcast part of the prefix differs, so the files never share a group.
//
// Pairing is evidence-based and direction-agnostic: a stale podcast (its
// podcasts-table last_seen no longer refreshed — podcast rows are updated by
// podcast title, so they are immune to the historical episode-row
// contamination) is paired with the live podcast that shares at least
// retitleMinMatches episodes matching on published date + sameEpisodeTitles.
// Each matched episode pair is then merged exactly like a twin group, with
// the live row's hash and canonical filename as the target.

const retitleMinMatches = 5

// loadPodcastLastSeen returns podcasts.title -> last_seen.
func loadPodcastLastSeen(dbFile string) (map[string]string, error) {
	out := make(map[string]string)
	db, err := sql.Open(sqlite3, dbFile)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.Query(`SELECT title, last_seen FROM podcasts;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var t, ls string
		if err := rows.Scan(&t, &ls); err != nil {
			return nil, err
		}
		out[t] = ls
	}
	return out, rows.Err()
}

func lastSeenIsLive(lastSeen string, now time.Time) bool {
	t, err := time.Parse(time.RFC3339, lastSeen)
	if err != nil {
		return true // err on the safe side
	}
	return now.Sub(t) <= dedupLivenessWindow
}

// planRetitles builds the merge plan for podcast-retitle duplicates. Pure,
// like planDedup.
func planRetitles(files []dedupFile, owners map[string]dedupOwner, url2ep map[string]string,
	podLastSeen map[string]string, now time.Time) []dedupAction {

	// Split episodes-table owners by podcast liveness (podcasts-table based)
	staleByPod := make(map[string][]dedupOwner)
	liveByPod := make(map[string][]dedupOwner)
	for _, o := range owners {
		if o.interactive {
			continue
		}
		ls, known := podLastSeen[o.podcastTitle]
		if !known {
			continue
		}
		if lastSeenIsLive(ls, now) {
			liveByPod[o.podcastTitle] = append(liveByPod[o.podcastTitle], o)
		} else {
			staleByPod[o.podcastTitle] = append(staleByPod[o.podcastTitle], o)
		}
	}

	date10 := func(s string) string {
		if len(s) >= 10 {
			return s[:10]
		}
		return s
	}

	// Index live rows by (podcast, published date) for matching
	liveByPodDate := make(map[string]map[string][]dedupOwner)
	for pod, rows := range liveByPod {
		m := make(map[string][]dedupOwner)
		for _, o := range rows {
			d := date10(o.published)
			m[d] = append(m[d], o)
		}
		liveByPodDate[pod] = m
	}

	// For each stale podcast, find the live podcast with the most episode
	// matches; keep per-stale-row match lists for the chosen pairing.
	type epMatch struct {
		old     dedupOwner
		matches []dedupOwner
	}
	stalePods := make([]string, 0, len(staleByPod))
	for p := range staleByPod {
		stalePods = append(stalePods, p)
	}
	sort.Strings(stalePods)

	filesByEp := make(map[string][]dedupFile)
	filesByPrefix := make(map[string][]dedupFile)
	for _, f := range files {
		if _, ok := owners[f.hash]; ok {
			f.epHash = f.hash
		} else if ep, ok := url2ep[f.hash]; ok {
			f.epHash = ep
		}
		if f.epHash != "" {
			filesByEp[f.epHash] = append(filesByEp[f.epHash], f)
		} else {
			filesByPrefix[f.prefix] = append(filesByPrefix[f.prefix], f)
		}
	}

	actions := make([]dedupAction, 0)
	for _, sp := range stalePods {
		bestPod := ""
		var bestMatches []epMatch
		livePods := make([]string, 0, len(liveByPodDate))
		for lp := range liveByPodDate {
			livePods = append(livePods, lp)
		}
		sort.Strings(livePods)
		for _, lp := range livePods {
			ms := make([]epMatch, 0)
			matched := 0
			for _, old := range staleByPod[sp] {
				cands := liveByPodDate[lp][date10(old.published)]
				hits := make([]dedupOwner, 0, 1)
				for _, c := range cands {
					if sameEpisodeTitles(old.title, c.title) {
						hits = append(hits, c)
					}
				}
				if len(hits) > 0 {
					matched++
					ms = append(ms, epMatch{old: old, matches: hits})
				}
			}
			if matched >= retitleMinMatches && (bestPod == "" || matched > len(bestMatches)) {
				bestPod = lp
				bestMatches = ms
			}
		}
		if bestPod == "" {
			continue
		}

		// Unknown-hash files attach to a merged pair by canonical-prefix
		// match — but only when exactly one pair owns that prefix. Same-day
		// "pt 1"/"pt 2" siblings share a digit-stripped prefix, and guessing
		// which part an unattributable file holds would risk deleting a
		// distinct episode.
		prefixPairs := make(map[string]int)
		pairPrefixes := make([][]string, len(bestMatches))
		for i, m := range bestMatches {
			seen := make(map[string]bool)
			cands := []dedupOwner{m.old}
			if len(m.matches) == 1 {
				cands = append(cands, m.matches[0])
			}
			for _, o := range cands {
				cn := canonicalNameFor(o)
				if cn == "" {
					continue
				}
				if nmh, ok := nameMinusHash(cn); ok && !seen[nmh] {
					seen[nmh] = true
					prefixPairs[nmh]++
					pairPrefixes[i] = append(pairPrefixes[i], nmh)
				}
			}
		}

		for i, m := range bestMatches {
			oldFiles := filesByEp[m.old.epHash]
			if len(m.matches) != 1 {
				for _, f := range oldFiles {
					actions = append(actions, dedupAction{kind: actManual, file: f,
						reason: fmt.Sprintf("retitle: %q matches %d live episodes in %q", m.old.title, len(m.matches), bestPod)})
				}
				continue
			}
			live := m.matches[0]

			group := make([]dedupFile, 0, 4)
			for _, f := range oldFiles {
				if !m.old.interactive {
					f.pruneEp = m.old.epHash
				}
				group = append(group, f)
			}
			group = append(group, filesByEp[live.epHash]...)
			for _, nmh := range pairPrefixes[i] {
				loose := filesByPrefix[nmh]
				if len(loose) == 0 {
					continue
				}
				if prefixPairs[nmh] != 1 {
					for _, f := range loose {
						actions = append(actions, dedupAction{kind: actManual, file: f,
							reason: fmt.Sprintf("retitle: unattributable copy; %d merged pairs share prefix %s", prefixPairs[nmh], nmh)})
					}
					delete(filesByPrefix, nmh)
					continue
				}
				group = append(group, loose...)
				delete(filesByPrefix, nmh)
			}
			if len(group) < 2 {
				continue
			}
			dedupCopies(group, live.epHash, canonicalNameFor(live), &actions)
		}
	}
	return actions
}

// runDedupRetitles gathers files, plans the retitle merges, and executes.
func runDedupRetitles(scanPaths []string, apply bool) error {
	owners, url2ep, err := loadDedupOwners(dbFileName)
	if err != nil {
		return err
	}
	podLastSeen, err := loadPodcastLastSeen(dbFileName)
	if err != nil {
		return err
	}
	files, err := gatherDedupFiles(scanPaths)
	if err != nil {
		return err
	}
	actions := planRetitles(files, owners, url2ep, podLastSeen, time.Now())
	return executeDedupPlan(actions, scanPaths, apply, "--dedup-retitles-delete")
}
