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
)

var dedupNow = time.Date(2026, 7, 5, 12, 0, 0, 0, time.UTC)

func liveOwner(podcast, title, published string) dedupOwner {
	return dedupOwner{
		epHash:       fmt.Sprintf("%x", md5.Sum([]byte(podcast+title))),
		podcastTitle: podcast,
		title:        title,
		published:    published,
		lastSeen:     dedupNow.Add(-time.Hour).Format(time.RFC3339),
	}
}

func staleOwner(podcast, title, published string) dedupOwner {
	o := liveOwner(podcast, title, published)
	o.lastSeen = dedupNow.Add(-60 * 24 * time.Hour).Format(time.RFC3339)
	return o
}

func mkFile(dir, podcast, title, date, hash string, size int64) dedupFile {
	name := buildEpisodeFilenameWithHash(podcast, title, date, hash)
	prefix, ok := nameMinusHash(name)
	if !ok {
		panic("bad test filename: " + name)
	}
	return dedupFile{
		path:   filepath.Join(dir, name),
		dir:    dir,
		name:   name,
		hash:   hash,
		prefix: prefix,
		size:   size,
	}
}

func actionsByKind(actions []dedupAction) map[string][]dedupAction {
	out := make(map[string][]dedupAction)
	for _, a := range actions {
		out[a.kind] = append(out[a.kind], a)
	}
	return out
}

// A live episode plus a stale retitle twin (same digit sequence): the stale
// copy is a duplicate and the live canonical-named copy is kept.
func TestPlanDedupRetitleTwinDeleted(t *testing.T) {
	live := liveOwner("Book Show", "Peter Stark, Astoria!", "2022-03-28")
	stale := staleOwner("Book Show", "Peter Stark Astoria", "2022-03-28")
	owners := map[string]dedupOwner{live.epHash: live, stale.epHash: stale}

	liveFile := mkFile("/pods", "Book Show", live.title, "2022-03-28", live.epHash, 74039180)
	staleFile := mkFile("/pods", "Book Show", stale.title, "2022-03-28", stale.epHash, 74039181)
	if liveFile.prefix != staleFile.prefix {
		t.Fatalf("test setup: prefixes must collide, got %q vs %q", liveFile.prefix, staleFile.prefix)
	}

	actions := planDedup([]dedupFile{liveFile, staleFile}, owners, nil, dedupNow)
	byKind := actionsByKind(actions)

	if len(byKind[actDelete]) != 1 || byKind[actDelete][0].file.hash != stale.epHash {
		t.Fatalf("expected exactly the stale twin deleted, got %+v", actions)
	}
	if len(byKind[actManual])+len(byKind[actRename]) != 0 {
		t.Fatalf("expected no manual/rename actions, got %+v", actions)
	}
}

// Two live episodes ("Part 1"/"Part 2", same day) each with their canonical
// file: nothing to do, and definitely nothing deleted.
func TestPlanDedupDistinctLiveEpisodesUntouched(t *testing.T) {
	p1 := liveOwner("Analysis", "The Deobandis: Part 1", "2016-04-14")
	p2 := liveOwner("Analysis", "The Deobandis: Part 2", "2016-04-14")
	owners := map[string]dedupOwner{p1.epHash: p1, p2.epHash: p2}

	f1 := mkFile("/pods", "Analysis", p1.title, "2016-04-14", p1.epHash, 40361237)
	f2 := mkFile("/arch", "Analysis", p2.title, "2016-04-14", p2.epHash, 40894464)
	if f1.prefix != f2.prefix {
		t.Fatalf("test setup: prefixes must collide, got %q vs %q", f1.prefix, f2.prefix)
	}

	actions := planDedup([]dedupFile{f1, f2}, owners, nil, dedupNow)
	if len(actions) != 0 {
		t.Fatalf("expected no actions for two live episodes with one copy each, got %+v", actions)
	}
}

// A stale sibling with a DIFFERENT digit sequence is a genuinely different
// episode that fell out of the feed — never deleted, reported as manual.
func TestPlanDedupDigitMismatchGoesManual(t *testing.T) {
	p2 := liveOwner("Analysis", "The Deobandis: Part 2", "2016-04-14")
	p1 := staleOwner("Analysis", "The Deobandis: Part 1", "2016-04-14")
	owners := map[string]dedupOwner{p1.epHash: p1, p2.epHash: p2}

	f1 := mkFile("/arch", "Analysis", p1.title, "2016-04-14", p1.epHash, 40361237)
	f2 := mkFile("/arch", "Analysis", p2.title, "2016-04-14", p2.epHash, 40894464)

	actions := planDedup([]dedupFile{f1, f2}, owners, nil, dedupNow)
	byKind := actionsByKind(actions)
	if len(byKind[actDelete])+len(byKind[actStub]) != 0 {
		t.Fatalf("expected no deletions with digit-mismatched siblings, got %+v", actions)
	}
	if len(byKind[actManual]) != 1 || byKind[actManual][0].file.hash != p1.epHash {
		t.Fatalf("expected the stale sibling reported manual, got %+v", actions)
	}
}

// The Patreon-stub shape: a ~150-byte canonical-named stub next to a big
// legacy copy with an unknown (rotated-URL) hash. The stub goes, the legacy
// copy is kept and renamed to the canonical episode-hash filename.
func TestPlanDedupStubPlusUnknownLegacy(t *testing.T) {
	ep := liveOwner("Pat Show", "Secret Episode", "2020-01-01")
	owners := map[string]dedupOwner{ep.epHash: ep}

	stub := mkFile("/pods", "Pat Show", ep.title, "2020-01-01", ep.epHash, 148)
	legacy := mkFile("/arch", "Pat Show", ep.title, "2020-01-01", strings.Repeat("9", 32), 10240000)

	actions := planDedup([]dedupFile{stub, legacy}, owners, nil, dedupNow)
	byKind := actionsByKind(actions)

	if len(byKind[actStub]) != 1 || byKind[actStub][0].file.hash != ep.epHash {
		t.Fatalf("expected the canonical-named stub deleted, got %+v", actions)
	}
	if len(byKind[actRename]) != 1 {
		t.Fatalf("expected the legacy keeper renamed, got %+v", actions)
	}
	wantName := buildEpisodeFilenameWithHash("Pat Show", ep.title, "2020-01-01", ep.epHash)
	if filepath.Base(byKind[actRename][0].newPath) != wantName {
		t.Fatalf("expected rename to %q, got %q", wantName, byKind[actRename][0].newPath)
	}
	if byKind[actRename][0].newPath != filepath.Join("/arch", wantName) {
		t.Fatalf("expected rename within keeper's dir, got %q", byKind[actRename][0].newPath)
	}
}

// Two legacy copies, one attributable via file_url_hash: single-owner group,
// keep the largest, rename it to canonical, delete the other.
func TestPlanDedupRotatedURLPair(t *testing.T) {
	ep := liveOwner("Leg Show", "Old One", "2019-09-09")
	owners := map[string]dedupOwner{ep.epHash: ep}
	currentURLHash := strings.Repeat("f", 32)
	url2ep := map[string]string{currentURLHash: ep.epHash}

	resolved := mkFile("/arch", "Leg Show", ep.title, "2019-09-09", currentURLHash, 7168000)
	unknown := mkFile("/arch", "Leg Show", ep.title, "2019-09-09", strings.Repeat("8", 32), 7680000)

	actions := planDedup([]dedupFile{resolved, unknown}, owners, url2ep, dedupNow)
	byKind := actionsByKind(actions)

	if len(byKind[actDelete]) != 1 || byKind[actDelete][0].file.hash != currentURLHash {
		t.Fatalf("expected smaller resolved copy deleted, got %+v", actions)
	}
	if len(byKind[actRename]) != 1 || byKind[actRename][0].file.hash != strings.Repeat("8", 32) {
		t.Fatalf("expected unknown-hash keeper renamed to canonical, got %+v", actions)
	}
}

// No db knowledge at all: keep the largest, delete a stub next to it, no
// rename possible.
func TestPlanDedupUnknownOnlyGroup(t *testing.T) {
	stub := mkFile("/pods", "Ghost Show", "Ep", "2018-01-01", strings.Repeat("1", 32), 200)
	big := mkFile("/pods", "Ghost Show", "Ep", "2018-01-01", strings.Repeat("2", 32), 5000000)

	actions := planDedup([]dedupFile{stub, big}, map[string]dedupOwner{}, nil, dedupNow)
	byKind := actionsByKind(actions)
	if len(byKind[actStub]) != 1 || byKind[actStub][0].file.hash != strings.Repeat("1", 32) {
		t.Fatalf("expected stub deleted, got %+v", actions)
	}
	if len(byKind[actRename]) != 0 {
		t.Fatalf("expected no rename without db knowledge, got %+v", actions)
	}
}

// Unattributable copies in a group where two live episodes collide are never
// guessed at.
func TestPlanDedupUnattributableWithLiveSiblingsManual(t *testing.T) {
	p1 := liveOwner("Analysis", "The Deobandis: Part 1", "2016-04-14")
	p2 := liveOwner("Analysis", "The Deobandis: Part 2", "2016-04-14")
	owners := map[string]dedupOwner{p1.epHash: p1, p2.epHash: p2}

	f1 := mkFile("/pods", "Analysis", p1.title, "2016-04-14", p1.epHash, 40361237)
	f2 := mkFile("/arch", "Analysis", p2.title, "2016-04-14", p2.epHash, 40894464)
	mystery := mkFile("/arch", "Analysis", p1.title, "2016-04-14", strings.Repeat("3", 32), 39845888)

	actions := planDedup([]dedupFile{f1, f2, mystery}, owners, nil, dedupNow)
	byKind := actionsByKind(actions)
	if len(byKind[actDelete])+len(byKind[actStub]) != 0 {
		t.Fatalf("expected no deletions, got %+v", actions)
	}
	if len(byKind[actManual]) != 1 || byKind[actManual][0].file.hash != strings.Repeat("3", 32) {
		t.Fatalf("expected the mystery copy reported manual, got %+v", actions)
	}
}

// End-to-end: apply mode deletes/renames on disk and maintains downloads and
// archived_episodes inside one transaction.
func TestRunDedupTwinsApply(t *testing.T) {
	tmpDir := useTempWorkingDir(t)
	createTablesIfNotExist()

	archDir := t.TempDir()

	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	podcast := "Pat Show"
	title := "Secret Episode"
	epHash := fmt.Sprintf("%x", md5.Sum([]byte(podcast+title)))
	legacyHash := strings.Repeat("9", 32)

	stubName := buildEpisodeFilenameWithHash(podcast, title, "2020-01-01", epHash)
	legacyName := buildEpisodeFilenameWithHash(podcast, title, "2020-01-01", legacyHash)
	if err := os.WriteFile(filepath.Join(tmpDir, stubName), []byte("errorpage"), 0666); err != nil {
		t.Fatalf("write stub: %v", err)
	}
	if err := os.WriteFile(filepath.Join(archDir, legacyName), make([]byte, 2000000), 0666); err != nil {
		t.Fatalf("write legacy: %v", err)
	}

	nowStr := time.Now().Format(time.RFC3339)
	if _, err := db.Exec(`INSERT INTO episodes (title, published, first_seen, last_seen, podcast_title,
		podcastname_episodename_hash, file_url_hash, file)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?);`,
		title, "2020-01-01T00:00:00Z", nowStr, nowStr, podcast, epHash, "cafe"+strings.Repeat("0", 28), "https://x/cur.mp3"); err != nil {
		t.Fatalf("insert episode: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO downloads (filename, hash, first_seen, last_seen)
		VALUES (?, ?, ?, ?), (?, ?, ?, ?);`,
		stubName, epHash, nowStr, nowStr, legacyName, legacyHash, nowStr, nowStr); err != nil {
		t.Fatalf("insert downloads: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO archived_episodes (podcastname_episodename_hash, archived_path, archived_at)
		VALUES (?, ?, ?);`, legacyHash, filepath.Join(archDir, legacyName), nowStr); err != nil {
		t.Fatalf("insert archive row: %v", err)
	}

	if err := runDedupTwins([]string{tmpDir, archDir}, true); err != nil {
		t.Fatalf("runDedupTwins: %v", err)
	}

	// Stub gone; keeper renamed to canonical name in the archive dir
	if _, err := os.Stat(filepath.Join(tmpDir, stubName)); !os.IsNotExist(err) {
		t.Fatalf("expected stub removed, err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(archDir, legacyName)); !os.IsNotExist(err) {
		t.Fatalf("expected legacy name gone after rename, err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(archDir, stubName)); err != nil {
		t.Fatalf("expected keeper under canonical name, err=%v", err)
	}

	// downloads rows for both old names pruned
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM downloads WHERE filename IN (?, ?);`, stubName, legacyName).Scan(&n); err != nil {
		t.Fatalf("query downloads: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected both downloads rows pruned, got %d", n)
	}

	// archive registry now keyed by the episode hash and pointing at the new path
	var path string
	if err := db.QueryRow(`SELECT archived_path FROM archived_episodes WHERE podcastname_episodename_hash = ?;`, epHash).Scan(&path); err != nil {
		t.Fatalf("expected registry row under episode hash: %v", err)
	}
	if path != filepath.Join(archDir, stubName) {
		t.Fatalf("registry path = %q, want %q", path, filepath.Join(archDir, stubName))
	}
	if err := db.QueryRow(`SELECT count(*) FROM archived_episodes WHERE podcastname_episodename_hash = ?;`, legacyHash).Scan(&n); err != nil {
		t.Fatalf("query old registry row: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected old registry row removed, got %d", n)
	}
}

func TestSameEpisodeTitles(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"#199 Esther Perel: Cultivating Desire", "Esther Perel: Cultivating Desire", true},
		{"Esther Perel: Cultivating Desire | #199", "Esther Perel: Cultivating Desire", true},
		{"#199 Esther Perel: Cultivating Desire (2019)", "Esther Perel: Cultivating Desire", true},
		{"/391/ The Biggest Country ft. Vedi Hadiz", "/390/ The Biggest Country ft. Vedi Hadiz", true},
		{"#235 - Opus 4.6, GPT-5.3-codex", "#234 - Opus 4.6, GPT-5.3-codex", true},
		{"#181 Dr. Gio Valiante (Part 2)", "Dr. Gio Valiante (Part 2)", true},
		{"#188: Bryan Johnson: Five Habits", "Bryan Johnson: Five Habits", true},
		{"The Deobandis: Part 1", "The Deobandis: Part 2", false},
		{"#181 Dr. Gio Valiante (Part 2)", "Dr. Gio Valiante (Part 1)", false},
		{"2 Fast 2 Furious Review", "Fast Furious Review", false},
	}
	for _, c := range cases {
		if got := sameEpisodeTitles(c.a, c.b); got != c.want {
			t.Errorf("sameEpisodeTitles(%q, %q) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}

// A renumbered-title stale variant that is a stub: deleted, and its stale
// episodes row marked for pruning so it can't re-queue for download.
func TestPlanDedupRenumberedStubPruned(t *testing.T) {
	live := liveOwner("TKP", "Esther Perel: Cultivating Desire", "2024-07-23")
	stale := staleOwner("TKP", "#199 Esther Perel: Cultivating Desire", "2024-07-23")
	owners := map[string]dedupOwner{live.epHash: live, stale.epHash: stale}

	liveFile := mkFile("/pods", "TKP", live.title, "2024-07-23", live.epHash, 112316401)
	stubFile := mkFile("/pods", "TKP", stale.title, "2024-07-23", stale.epHash, 163)
	if liveFile.prefix != stubFile.prefix {
		t.Fatalf("test setup: prefixes must collide, got %q vs %q", liveFile.prefix, stubFile.prefix)
	}

	actions := planDedup([]dedupFile{liveFile, stubFile}, owners, nil, dedupNow)
	byKind := actionsByKind(actions)
	if len(byKind[actStub]) != 1 || byKind[actStub][0].file.hash != stale.epHash {
		t.Fatalf("expected renumbered stub deleted, got %+v", actions)
	}
	if byKind[actStub][0].pruneEp != stale.epHash {
		t.Fatalf("expected stale episodes row marked for pruning, got %+v", byKind[actStub][0])
	}
	if len(byKind[actManual]) != 0 {
		t.Fatalf("expected no manual actions, got %+v", actions)
	}
}

// A renumbered-title stale variant is the same episode even when the sizes
// diverge (feeds re-encode; July 2026 data showed a uniform ~1.5x ratio).
// When the stale copy is the larger one, it becomes the keeper and is renamed
// to the canonical name; the smaller live-canonical copy is deleted.
func TestPlanDedupRenumberedKeepsLargerCopy(t *testing.T) {
	live := liveOwner("TKP", "TKP Insights: Leadership", "2022-08-01")
	stale := staleOwner("TKP", "#160 TKP Insights: Leadership", "2022-08-01")
	owners := map[string]dedupOwner{live.epHash: live, stale.epHash: stale}

	liveFile := mkFile("/pods", "TKP", live.title, "2022-08-01", live.epHash, 52000000)
	bigger := mkFile("/pods", "TKP", stale.title, "2022-08-01", stale.epHash, 80000000)

	actions := planDedup([]dedupFile{liveFile, bigger}, owners, nil, dedupNow)
	byKind := actionsByKind(actions)
	if len(byKind[actDelete]) != 1 || byKind[actDelete][0].file.hash != live.epHash {
		t.Fatalf("expected the smaller live copy deleted in favour of the larger original, got %+v", actions)
	}
	if len(byKind[actRename]) != 1 || byKind[actRename][0].file.hash != stale.epHash ||
		byKind[actRename][0].pruneEp != stale.epHash {
		t.Fatalf("expected larger stale keeper renamed to canonical with row prune, got %+v", actions)
	}
	wantName := buildEpisodeFilenameWithHash("TKP", live.title, "2022-08-01", live.epHash)
	if filepath.Base(byKind[actRename][0].newPath) != wantName {
		t.Fatalf("expected rename to %q, got %q", wantName, byKind[actRename][0].newPath)
	}
	if len(byKind[actManual]) != 0 {
		t.Fatalf("expected no manual actions, got %+v", actions)
	}
}

// pruneStaleEpisodes: stale rows with no surviving copy anywhere are pruned;
// rows covered by a legacy-named file (via file_url_hash) or the archive
// registry are kept, as are live rows.
func TestPruneStaleEpisodes(t *testing.T) {
	tmpDir := useTempWorkingDir(t)
	createTablesIfNotExist()

	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	nowStr := time.Now().Format(time.RFC3339)
	oldStr := time.Now().Add(-60 * 24 * time.Hour).Format(time.RFC3339)

	legacyURLHash := strings.Repeat("a", 32)
	legacyName := buildEpisodeFilenameWithHash("Show", "Covered By Legacy", "2020-01-01", legacyURLHash)
	if err := os.WriteFile(filepath.Join(tmpDir, legacyName), []byte("x"), 0666); err != nil {
		t.Fatalf("write legacy file: %v", err)
	}

	archivedHash := strings.Repeat("b", 32)
	if _, err := db.Exec(`INSERT INTO archived_episodes (podcastname_episodename_hash, archived_path, archived_at)
		VALUES (?, ?, ?);`, archivedHash, "/unmounted/x.mp3", nowStr); err != nil {
		t.Fatalf("insert archive row: %v", err)
	}

	ins := func(title, epHash, urlHash, lastSeen string) {
		if _, err := db.Exec(`INSERT INTO episodes (title, published, first_seen, last_seen, podcast_title,
			podcastname_episodename_hash, file_url_hash, file)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?);`,
			title, "2020-01-01T00:00:00Z", oldStr, lastSeen, "Show", epHash, urlHash, "https://x/"+title+".mp3"); err != nil {
			t.Fatalf("insert %s: %v", title, err)
		}
	}
	ins("Covered By Legacy", strings.Repeat("1", 32), legacyURLHash, oldStr)
	ins("Covered By Registry", archivedHash, strings.Repeat("c", 32), oldStr)
	ins("Live Missing", strings.Repeat("2", 32), strings.Repeat("d", 32), nowStr)
	ins("Stale Orphan", strings.Repeat("3", 32), strings.Repeat("e", 32), oldStr)

	if err := pruneStaleEpisodes([]string{tmpDir}, true); err != nil {
		t.Fatalf("pruneStaleEpisodes: %v", err)
	}

	var titles []string
	rows, err := db.Query(`SELECT title FROM episodes ORDER BY title;`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		titles = append(titles, s)
	}
	rows.Close()
	want := []string{"Covered By Legacy", "Covered By Registry", "Live Missing"}
	if strings.Join(titles, "|") != strings.Join(want, "|") {
		t.Fatalf("surviving rows = %v, want %v", titles, want)
	}
}

func retitleTestData(oldPod, newPod string, n int) (map[string]dedupOwner, map[string]string, []dedupOwner, []dedupOwner) {
	owners := make(map[string]dedupOwner)
	oldRows := make([]dedupOwner, 0, n)
	newRows := make([]dedupOwner, 0, n)
	for i := 0; i < n; i++ {
		date := fmt.Sprintf("2023-01-%02d", i+1)
		title := fmt.Sprintf("Guest %c: A Conversation", 'A'+i)
		or := staleOwner(oldPod, title, date)
		nr := liveOwner(newPod, title, date)
		owners[or.epHash] = or
		owners[nr.epHash] = nr
		oldRows = append(oldRows, or)
		newRows = append(newRows, nr)
	}
	podLastSeen := map[string]string{
		oldPod: dedupNow.Add(-90 * 24 * time.Hour).Format(time.RFC3339),
		newPod: dedupNow.Add(-time.Hour).Format(time.RFC3339),
	}
	return owners, podLastSeen, oldRows, newRows
}

// A renamed podcast with both back catalogs on disk: each old-prefix copy is
// merged into the live episode — the larger copy wins and ends up under the
// live canonical name, and the old row is pruned.
func TestPlanRetitlesMergesAcrossPodcastRename(t *testing.T) {
	owners, podLastSeen, oldRows, newRows := retitleTestData("Old Show", "New Show", 6)

	files := make([]dedupFile, 0)
	for i := range oldRows {
		files = append(files, mkFile("/pods", "Old Show", oldRows[i].title, oldRows[i].published, oldRows[i].epHash, 110000000))
		files = append(files, mkFile("/pods", "New Show", newRows[i].title, newRows[i].published, newRows[i].epHash, 75000000))
	}

	actions := planRetitles(files, owners, nil, podLastSeen, dedupNow)
	byKind := actionsByKind(actions)

	if len(byKind[actDelete]) != 6 {
		t.Fatalf("expected 6 deletions (smaller live copies), got %+v", byKind[actDelete])
	}
	for _, a := range byKind[actDelete] {
		if owners[a.file.hash].podcastTitle != "New Show" {
			t.Fatalf("expected the smaller new-prefix copies deleted, got %+v", a.file)
		}
	}
	if len(byKind[actRename]) != 6 {
		t.Fatalf("expected 6 keeper renames to live canonical, got %d", len(byKind[actRename]))
	}
	for _, a := range byKind[actRename] {
		if owners[a.file.hash].podcastTitle != "Old Show" || a.pruneEp != a.file.hash {
			t.Fatalf("expected old-prefix keeper renamed with its stale row pruned, got %+v", a)
		}
		if !strings.HasPrefix(filepath.Base(a.newPath), "New_Show-") {
			t.Fatalf("expected rename to live canonical name, got %q", a.newPath)
		}
	}
	if len(byKind[actManual]) != 0 {
		t.Fatalf("expected no manual actions, got %+v", byKind[actManual])
	}
}

// Below the pairing threshold nothing is touched: a coincidental shared
// episode between unrelated podcasts must not trigger merging.
func TestPlanRetitlesRequiresEnoughMatches(t *testing.T) {
	owners, podLastSeen, oldRows, newRows := retitleTestData("Old Show", "Unrelated Show", retitleMinMatches-1)

	files := make([]dedupFile, 0)
	for i := range oldRows {
		files = append(files, mkFile("/pods", "Old Show", oldRows[i].title, oldRows[i].published, oldRows[i].epHash, 110000000))
		files = append(files, mkFile("/pods", "Unrelated Show", newRows[i].title, newRows[i].published, newRows[i].epHash, 75000000))
	}

	actions := planRetitles(files, owners, nil, podLastSeen, dedupNow)
	if len(actions) != 0 {
		t.Fatalf("expected no actions below the match threshold, got %+v", actions)
	}
}

// An old row matching two same-day live episodes is never guessed at.
func TestPlanRetitlesAmbiguousMatchManual(t *testing.T) {
	owners, podLastSeen, oldRows, _ := retitleTestData("Old Show", "New Show", 6)

	// Add a second live episode on day 1 whose title also matches old row 1
	// after number stripping
	dup := liveOwner("New Show", "#7 "+oldRows[0].title, oldRows[0].published)
	owners[dup.epHash] = dup

	files := []dedupFile{
		mkFile("/pods", "Old Show", oldRows[0].title, oldRows[0].published, oldRows[0].epHash, 110000000),
	}
	for i := range oldRows {
		nr := liveOwner("New Show", oldRows[i].title, oldRows[i].published)
		files = append(files, mkFile("/pods", "New Show", nr.title, nr.published, nr.epHash, 75000000))
	}

	actions := planRetitles(files, owners, nil, podLastSeen, dedupNow)
	byKind := actionsByKind(actions)
	for _, a := range byKind[actDelete] {
		if a.file.hash == oldRows[0].epHash {
			t.Fatalf("ambiguous old copy must not be deleted: %+v", a)
		}
	}
	found := false
	for _, a := range byKind[actManual] {
		if a.file.hash == oldRows[0].epHash {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected ambiguous old copy reported manual, got %+v", actions)
	}
}

// The episodes last_seen refresh must be scoped to the episode's own row, not
// every same-titled row across podcasts (the bug that kept "Aufhebunga Bunga
// (Patreon)" rows looking live years after the feed became "Bungacast").
func TestEpisodeLastSeenUpdateScopedToPodcast(t *testing.T) {
	useTempWorkingDir(t)
	createTablesIfNotExist()

	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	oldStr := "2024-02-19T01:00:00Z"
	title := "/222/ Nukes 4 Kids ft. Emmet Penney, pt. 1"
	abHash := fmt.Sprintf("%x", md5.Sum([]byte("Aufhebunga Bunga (Patreon)"+title)))
	bcHash := fmt.Sprintf("%x", md5.Sum([]byte("Bungacast (Patreon feed)"+title)))
	for _, r := range []struct{ pod, hash string }{
		{"Aufhebunga Bunga (Patreon)", abHash},
		{"Bungacast (Patreon feed)", bcHash},
	} {
		if _, err := db.Exec(`INSERT INTO episodes (title, published, first_seen, last_seen, podcast_title,
			podcastname_episodename_hash, file_url_hash, file)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?);`,
			title, "2021-11-02T00:00:00Z", oldStr, oldStr, r.pod, r.hash, "u"+r.hash[1:], "https://x/"+r.hash+".mp3"); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	pod := map[string]string{"title": "Bungacast (Patreon feed)", "author": "a", "category": "c",
		"description": "d", "language": "l", "link": "k"}
	episodes := []M{{"title": title, "file": "https://x/new.mp3", "guid": "g1", "published": "2021-11-02T00:00:00Z"}}
	podEpisodesIntoDatabase(db, pod, episodes)

	var abSeen, bcSeen string
	if err := db.QueryRow(`SELECT last_seen FROM episodes WHERE podcastname_episodename_hash=?;`, abHash).Scan(&abSeen); err != nil {
		t.Fatalf("query ab: %v", err)
	}
	if err := db.QueryRow(`SELECT last_seen FROM episodes WHERE podcastname_episodename_hash=?;`, bcHash).Scan(&bcSeen); err != nil {
		t.Fatalf("query bc: %v", err)
	}
	if abSeen != oldStr {
		t.Fatalf("Aufhebunga row was refreshed by a Bungacast parse: last_seen=%q", abSeen)
	}
	if bcSeen == oldStr {
		t.Fatalf("Bungacast row should have been refreshed, still %q", bcSeen)
	}
}

// An unknown-hash old-prefix file whose digit-stripped prefix is shared by
// two merged pairs (same-day pt 1/pt 2) must go to manual, not be guessed
// into either pair. This is the Nukes & Kids 68MB-file shape.
func TestPlanRetitlesSharedPrefixLooseFileManual(t *testing.T) {
	owners, podLastSeen, oldRows, newRows := retitleTestData("Old Show", "New Show", 6)

	// Same-day two-part episode present on both sides (titles identical bar
	// the "/N/" numbering the new feed added, as in the real Bungacast data)
	oldP1 := staleOwner("Old Show", "Nukes 4 Kids ft. Emmet Penney, pt. 1", "2021-11-02")
	oldP2 := staleOwner("Old Show", "Nukes 4 Kids ft. Emmet Penney, pt. 2", "2021-11-02")
	newP1 := liveOwner("New Show", "/222/ Nukes 4 Kids ft. Emmet Penney, pt. 1", "2021-11-02")
	newP2 := liveOwner("New Show", "/223/ Nukes 4 Kids ft. Emmet Penney, pt. 2", "2021-11-02")
	for _, o := range []dedupOwner{oldP1, oldP2, newP1, newP2} {
		owners[o.epHash] = o
	}

	files := make([]dedupFile, 0)
	for i := range oldRows {
		files = append(files, mkFile("/pods", "Old Show", oldRows[i].title, oldRows[i].published, oldRows[i].epHash, 110000000))
		files = append(files, mkFile("/pods", "New Show", newRows[i].title, newRows[i].published, newRows[i].epHash, 75000000))
	}
	// The mystery file: old-prefix, unknown hash, same prefix as both parts
	mystery := mkFile("/pods", "Old Show", oldP1.title, "2021-11-02", strings.Repeat("7", 32), 68099268)
	mystery2 := mkFile("/pods", "Old Show", oldP2.title, "2021-11-02", strings.Repeat("6", 32), 68099268)
	if mystery.prefix != mystery2.prefix {
		t.Fatalf("test setup: pt1/pt2 prefixes must collide, got %q vs %q", mystery.prefix, mystery2.prefix)
	}
	files = append(files, mystery)

	actions := planRetitles(files, owners, nil, podLastSeen, dedupNow)
	byKind := actionsByKind(actions)
	for _, a := range byKind[actDelete] {
		if a.file.hash == strings.Repeat("7", 32) {
			t.Fatalf("mystery file must not be deleted: %+v", a)
		}
	}
	for _, a := range byKind[actRename] {
		if a.file.hash == strings.Repeat("7", 32) {
			t.Fatalf("mystery file must not be renamed: %+v", a)
		}
	}
	found := false
	for _, a := range byKind[actManual] {
		if a.file.hash == strings.Repeat("7", 32) {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected mystery file reported manual, got %+v", actions)
	}
}

// Digit-free titles must NOT match on vacuous digit equality — the bug behind
// the 2026-07-05 false retitle merges (unrelated daily feeds paired, same-day
// episodes with entirely different titles merged).
func TestSameEpisodeTitlesRequiresText(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"The Peasant Theory of Communist Brutality", "Israel accused of implementing starvation plan in Gaza", false},
		{"France on fire", "France on fire", true},
		{"France on fire", "Paris burning", false},
	}
	for _, c := range cases {
		if got := sameEpisodeTitles(c.a, c.b); got != c.want {
			t.Errorf("sameEpisodeTitles(%q, %q) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}

// Two unrelated daily feeds publishing digit-free titles on the same days
// must never be paired, no matter how many dates coincide.
func TestPlanRetitlesUnrelatedDailyFeedsNotPaired(t *testing.T) {
	owners := make(map[string]dedupOwner)
	files := make([]dedupFile, 0)
	oldTitles := []string{"The Peasant Theory", "Closing Arguments Live", "The OnlyFans Aristocrats",
		"Cancel Culture Comes", "The Policy Panacea", "How Much Truth"}
	newTitles := []string{"UK Budget spooks bond markets", "Higher for even longer", "The man behind the attack",
		"Can athletics vault", "PwC audit comes back", "What ETFs mean"}
	for i := 0; i < 6; i++ {
		date := fmt.Sprintf("2024-03-%02d", i+1)
		or := staleOwner("Dead Private Feed", oldTitles[i], date)
		nr := liveOwner("Live News Feed", newTitles[i], date)
		owners[or.epHash] = or
		owners[nr.epHash] = nr
		files = append(files, mkFile("/pods", "Dead Private Feed", or.title, date, or.epHash, 90000000))
		files = append(files, mkFile("/pods", "Live News Feed", nr.title, date, nr.epHash, 10000000))
	}
	podLastSeen := map[string]string{
		"Dead Private Feed": dedupNow.Add(-90 * 24 * time.Hour).Format(time.RFC3339),
		"Live News Feed":    dedupNow.Add(-time.Hour).Format(time.RFC3339),
	}

	actions := planRetitles(files, owners, nil, podLastSeen, dedupNow)
	if len(actions) != 0 {
		t.Fatalf("expected no actions for unrelated feeds, got %+v", actions)
	}
}
