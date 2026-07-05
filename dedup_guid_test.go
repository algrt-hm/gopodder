package main

import (
	"strings"
	"testing"
)

func guidOwner(epHash, podcast, title, published, lastSeen, guid string) dedupOwner {
	return dedupOwner{epHash: epHash, podcastTitle: podcast, title: title,
		published: published, lastSeen: lastSeen, guid: guid}
}

func guidFile(name, hash string, size int64) dedupFile {
	prefix, _ := nameMinusHash(name)
	return dedupFile{path: "/pods/" + name, dir: "/pods", name: name,
		hash: hash, prefix: prefix, size: size}
}

func TestPlanGuidDedupMergesRetitles(t *testing.T) {
	// The Vlad Tenev shape: retitled copy downloaded next to the original;
	// the fresher row's copy is kept, the stale row's copy deleted and its
	// episodes row pruned
	pod, pub := "The Knowledge Project", "2026-03-03T09:00:00Z"
	oldHash := strings.Repeat("a", 32)
	newHash := strings.Repeat("b", 32)
	owners := map[string]dedupOwner{
		oldHash: guidOwner(oldHash, pod, "Inside the Mind of Robinhood Co-Founder Vlad Tenev", pub, "2026-04-21T00:00:00Z", "g-tenev"),
		newHash: guidOwner(newHash, pod, "The Near Death Experience of RobinHood | Vlad Tenev, Co-Founder", pub, "2026-07-05T00:00:00Z", "g-tenev"),
	}
	newName := buildNonInteractiveFilename(pod, owners[newHash].title, pub, newHash)
	oldName := buildNonInteractiveFilename(pod, owners[oldHash].title, pub, oldHash)
	files := []dedupFile{
		guidFile(oldName, oldHash, 265005104),
		guidFile(newName, newHash, 265605661),
	}

	actions := planGuidDedup(files, owners, map[string]string{})
	if len(actions) != 1 {
		t.Fatalf("expected 1 action, got %d: %+v", len(actions), actions)
	}
	a := actions[0]
	if a.kind != actDelete || a.file.hash != oldHash {
		t.Errorf("expected delete of stale copy, got %+v", a)
	}
	if a.pruneEp != oldHash {
		t.Errorf("expected stale episodes row pruned, got %q", a.pruneEp)
	}
	if !strings.Contains(a.keeperPath, newHash) {
		t.Errorf("keeper should be the fresh row's copy, got %s", a.keeperPath)
	}
}

func TestPlanGuidDedupRenamesLoneStaleCopy(t *testing.T) {
	// Only the stale title's file exists: keep it but move it to the live
	// row's canonical filename so the download guard recognises it directly
	pod, pub := "P", "2026-05-01T00:00:00Z"
	oldHash := strings.Repeat("c", 32)
	newHash := strings.Repeat("d", 32)
	owners := map[string]dedupOwner{
		oldHash: guidOwner(oldHash, pod, "Old Title Words", pub, "2026-05-02T00:00:00Z", "g1"),
		newHash: guidOwner(newHash, pod, "New Title Words", pub, "2026-07-01T00:00:00Z", "g1"),
	}
	oldName := buildNonInteractiveFilename(pod, "Old Title Words", pub, oldHash)
	files := []dedupFile{guidFile(oldName, oldHash, 50000000)}

	actions := planGuidDedup(files, owners, map[string]string{})
	if len(actions) != 1 {
		t.Fatalf("expected 1 action, got %d: %+v", len(actions), actions)
	}
	a := actions[0]
	want := buildNonInteractiveFilename(pod, "New Title Words", pub, newHash)
	if a.kind != actRename || !strings.HasSuffix(a.newPath, want) {
		t.Errorf("expected rename to %s, got %+v", want, a)
	}
}

func TestPlanGuidDedupNoCorroborationIsManual(t *testing.T) {
	// Same guid but different dates and unrelated titles: a feed abusing
	// guids must not cause a deletion
	pod := "P"
	h1 := strings.Repeat("e", 32)
	h2 := strings.Repeat("f", 32)
	owners := map[string]dedupOwner{
		h1: guidOwner(h1, pod, "A Story About Bees", "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", "g-abused"),
		h2: guidOwner(h2, pod, "Completely Different Show Notes", "2026-06-01T00:00:00Z", "2026-07-01T00:00:00Z", "g-abused"),
	}
	files := []dedupFile{
		guidFile(buildNonInteractiveFilename(pod, owners[h1].title, owners[h1].published, h1), h1, 60000000),
		guidFile(buildNonInteractiveFilename(pod, owners[h2].title, owners[h2].published, h2), h2, 70000000),
	}

	actions := planGuidDedup(files, owners, map[string]string{})
	if len(actions) != 1 || actions[0].kind != actManual {
		t.Fatalf("expected single MANUAL action, got %+v", actions)
	}
	if actions[0].file.hash != h1 {
		t.Errorf("the stale, uncorroborated copy should be the manual one, got %+v", actions[0])
	}
}

func TestPlanGuidDedupIgnoresEmptyGuidAndOtherPodcasts(t *testing.T) {
	h1 := strings.Repeat("1", 32)
	h2 := strings.Repeat("2", 32)
	h3 := strings.Repeat("3", 32)
	h4 := strings.Repeat("4", 32)
	pub := "2026-05-01T00:00:00Z"
	owners := map[string]dedupOwner{
		// Empty guids never group
		h1: guidOwner(h1, "P", "T One", pub, "2026-05-02T00:00:00Z", ""),
		h2: guidOwner(h2, "P", "T Two", pub, "2026-05-02T00:00:00Z", ""),
		// Same guid string under different podcasts never groups
		h3: guidOwner(h3, "A", "T Three", pub, "2026-05-02T00:00:00Z", "shared"),
		h4: guidOwner(h4, "B", "T Four", pub, "2026-05-02T00:00:00Z", "shared"),
	}
	files := []dedupFile{
		guidFile(buildNonInteractiveFilename("P", "T One", pub, h1), h1, 60000000),
		guidFile(buildNonInteractiveFilename("P", "T Two", pub, h2), h2, 60000000),
		guidFile(buildNonInteractiveFilename("A", "T Three", pub, h3), h3, 60000000),
		guidFile(buildNonInteractiveFilename("B", "T Four", pub, h4), h4, 60000000),
	}
	if actions := planGuidDedup(files, owners, map[string]string{}); len(actions) != 0 {
		t.Errorf("expected no actions, got %+v", actions)
	}
}

func TestPlanGuidDedupLegacyURLHashAttribution(t *testing.T) {
	// A stale copy under a legacy url-hash filename still attributes to its
	// episode via url2ep and merges
	pod, pub := "P", "2026-05-01T00:00:00Z"
	oldHash := strings.Repeat("5", 32)
	newHash := strings.Repeat("6", 32)
	urlHash := strings.Repeat("7", 32)
	owners := map[string]dedupOwner{
		oldHash: guidOwner(oldHash, pod, "Old Words Here", pub, "2026-05-02T00:00:00Z", "g1"),
		newHash: guidOwner(newHash, pod, "New Words Here", pub, "2026-07-01T00:00:00Z", "g1"),
	}
	url2ep := map[string]string{urlHash: oldHash}
	files := []dedupFile{
		guidFile(buildNonInteractiveFilename(pod, "Old Words Here", pub, urlHash), urlHash, 60000000),
		guidFile(buildNonInteractiveFilename(pod, "New Words Here", pub, newHash), newHash, 60000000),
	}

	actions := planGuidDedup(files, owners, url2ep)
	if len(actions) != 1 || actions[0].kind != actDelete || actions[0].file.hash != urlHash {
		t.Fatalf("expected legacy copy deleted, got %+v", actions)
	}
}
