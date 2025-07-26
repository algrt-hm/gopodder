package main

import (
	"context"
	"crypto/tls"
	"net/http"
	"strings"
	"time"

	"github.com/mmcdole/gofeed" // RSS parsing
)

// isHttpError is a utility function to check if an error is a http error
func isHttpError(err error) bool {
	if err == nil {
		return false
	}

	// implied else
	return strings.Contains(err.Error(), "http error")
}

// parseLogic is the feed parsing logic
func parseLogic(feed *gofeed.Feed) (map[string]string, []M, error) {

	// Will throw the item maps in here
	var sItems []M
	pod := make(map[string]string)
	var err error = nil

	// Podcast metadata
	if len(feed.Authors) == 1 {
		pod[author] = strings.TrimSpace(feed.Authors[0].Name)
	} else {
		if len(feed.Authors) == 0 {
			// No authors
			if verbose {
				log.Println("No authors")
			}
		} else {
			log.Println("More than one author")
			log.Println(feed.Authors)
		}
	}

	pod[title] = strings.TrimSpace(feed.Title)
	pod[link] = strings.TrimSpace(feed.Link)
	pod[description] = strings.TrimSpace(feed.Description)
	pod[language_] = strings.TrimSpace(feed.Language)
	pod[category] = strings.TrimSpace(strings.Join(feed.Categories, ", "))

	// Episodes metadata
	for idx := range feed.Items {
		item := feed.Items[idx]

		// Looks like:
		//
		// type Item struct {
		//     Title           string                   `json:"title,omitempty"`
		//     Description     string                   `json:"description,omitempty"`
		//     Content         string                   `json:"content,omitempty"`
		//     Link            string                   `json:"link,omitempty"`
		//     Links           []string                 `json:"links,omitempty"`
		//     Updated         string                   `json:"updated,omitempty"`
		//     UpdatedParsed   *time.Time               `json:"updatedParsed,omitempty"`
		//     Published       string                   `json:"published,omitempty"`
		//     PublishedParsed *time.Time               `json:"publishedParsed,omitempty"`
		//     Author          *Person                  `json:"author,omitempty"` // Deprecated
		//     Authors         []*Person                `json:"authors,omitempty"`
		//     GUID            string                   `json:"guid,omitempty"`
		//     Image           *Image                   `json:"image,omitempty"`
		//     Categories      []string                 `json:"categories,omitempty"`
		//     Enclosures      []*Enclosure             `json:"enclosures,omitempty"`
		//     DublinCoreExt   *ext.DublinCoreExtension `json:"dcExt,omitempty"`
		//     ITunesExt       *ext.ITunesItemExtension `json:"itunesExt,omitempty"`
		//     Extensions      ext.Extensions           `json:"extensions,omitempty"`
		//     Custom          map[string]string        `json:"custom,omitempty"`
		// }

		// We want:
		//
		//    "title", "language", "itunes:author", "feed_url", "link", "description",
		//    "itunes:summary", "itunes:explicit", "enclosure"

		i := make(M)

		i[title] = strings.TrimSpace(item.Title)

		// Potential addition: use itunes:author if missing
		// Potential addition: Loop through for authors and handle >1
		if len(item.Authors) == 1 {
			i[author] = strings.TrimSpace(item.Authors[0].Name)
		} else {
			if len(item.Authors) == 0 {
				// No authors
				i[author] = ""
			} else {
				log.Println("More than one author")
				log.Println(item.Authors)
			}
		}

		i[link] = strings.TrimSpace(item.Link)
		i[description] = strings.TrimSpace(item.Description)
		i[guid] = strings.TrimSpace(item.GUID)

		// Change anything time.Time into a string
		// this is buffer value
		var t time.Time

		if item.UpdatedParsed != nil {
			t = *item.UpdatedParsed
			i[updated] = t.Format(time.RFC3339)
		} else {
			i[updated] = ""
		}

		if item.PublishedParsed != nil {
			t = *item.PublishedParsed
			i[published] = t.Format(time.RFC3339)
		} else {
			i[published] = ""
		}

		// Assumes only one enclosure
		if len(item.Enclosures) == 1 {
			i[file] = strings.TrimSpace(item.Enclosures[0].URL)
			i[format] = strings.TrimSpace(item.Enclosures[0].Type)
		} else {
			if len(item.Enclosures) == 0 {
				// Enclosures is empty
			} else {
				// If it's not empty and more than one then log
				log.Println("More than one enclosure")
				log.Println(item.Enclosures)
			}
		}

		// iTunes extension to spec (not always present)
		if item.ITunesExt != nil {
			// If author is an empty string (i.e. false) then use the iTunes author
			if i[author] == "" {
				i[author] = strings.TrimSpace(item.ITunesExt.Author)
			}

			// Pick up itunes episode while we are at in
			i[episode] = strings.TrimSpace(item.ITunesExt.Episode)

			// If desc is empty use itunes summary
			if i[description] == "" {
				i[description] = strings.TrimSpace(item.ITunesExt.Summary)
			}
		} else {
			// Set episode to empty string if we have not picked it up
			i[episode] = ""
		}

		sItems = append(sItems, i)
	}

	return pod, sItems, err
}

// parseFeed a function to to parse an individual RSS feed
func parseFeed(url string) (map[string]string, []M, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	log.Println("Parsing " + url)
	fp := gofeed.NewParser()

	fp.Client = &http.Client{
		// Extend the timeout a bit. See also: https://github.com/mmcdole/gofeed/issues/83#issuecomment-355485788
		Timeout: 60 * time.Second,
		// Allow various ciphers. See also: https://github.com/golang/go/issues/44267#issuecomment-819278575
		Transport: &http.Transport{
			TLSHandshakeTimeout: 10 * time.Second,
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS12,
				CipherSuites: []uint16{
					tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
					tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
					tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305, // Go 1.8 only
					tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,   // Go 1.8 only
					tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
					tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				},
			},
		}}

	// Change the user agent to something that looks like Chrome/Brave
	fp.UserAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"

	feed, err := fp.ParseURLWithContext(url, ctx)

	// If there is an error parsing the feed then return with the error
	if err != nil {
		return nil, nil, err
	}

	return parseLogic(feed)
}
