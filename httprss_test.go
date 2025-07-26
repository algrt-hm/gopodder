package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/mmcdole/gofeed" // RSS parsing
)

func TestParseLogic(t *testing.T) {
	// Open the test RSS file
	f, err := os.Open("./test.rss")
	if err != nil {
		t.Fatalf("failed to open test.rss: %v", err)
	}
	defer f.Close()

	// Parse the RSS feed using gofeed
	parser := gofeed.NewParser()
	feed, err := parser.Parse(f)
	if err != nil {
		t.Fatalf("failed to parse test.rss: %v", err)
	}

	// Call parseLogic
	pod, items, err := parseLogic(feed)
	if err != nil {
		t.Fatalf("parseLogic returned error: %v", err)
	}

	// Basic checks
	if pod == nil {
		t.Errorf("pod metadata is nil")
	}
	if len(items) == 0 {
		t.Errorf("no items parsed from feed")
	}

	// Check our maps have everything in them that we expect
	for idx, each := range items {
		// for each, print the keys
		// for k := range each {
		// 	fmt.Printf("idx: %v, key: %s\n", idx, k)
		// }

		/* Check that each of these keys are present
		key: title
		key: link
		key: updated
		key: published
		key: format
		key: episode
		key: author
		key: description
		key: guid
		key: file
		*/

		// Check that each required key is present
		requiredKeys := []string{"title", "link", "updated", "published", "format", "episode", "author", "description", "guid", "file"}
		for _, key := range requiredKeys {
			if _, exists := each[key]; !exists {
				t.Errorf("item %d missing required key: %s", idx, key)
			}

			// Print the first three
			if idx < 4 {
				fmt.Printf("%v: %s: %s\n", idx, key, each[key])
			}
		}

		// updated, episode, author are allowed to be empty strings
		// title, link, published, format, description, guid, file are not allowed to be empty strings
		// UPTO
	}
}
