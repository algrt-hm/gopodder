package main

import (
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
)

const initialListLimit = 10
const extraConfName = "gopodder-extra.conf"

// runInteractive launches the Bubble Tea UI and downloads selected episodes.
func runInteractive(defaultFolder string) error {
	if log != nil {
		prev := log.Writer()
		log.SetOutput(io.Discard)
		defer log.SetOutput(prev)
	}

	model := newInteractiveModel(defaultFolder)
	_, err := tea.NewProgram(model).Run()
	return err
}

type interactiveStep int

const (
	stepFeedSelect interactiveStep = iota
	stepURL
	stepLoading
	stepSelect
	stepFolder
	stepDownloading
	stepDone
)

type episodeItem struct {
	title    string
	date     time.Time
	dateStr  string
	url      string
	filename string
	selected bool
}

type feedParsedMsg struct {
	podTitle string
	episodes []episodeItem
	skipped  int
	err      error
}

type downloadResultMsg struct {
	index    int
	filename string
	err      error
}

type interactiveModel struct {
	step        interactiveStep
	urlInput    textinput.Model
	folderInput textinput.Model
	feedFile    string
	feedOptions []string
	feedCursor  int
	feedOffset  int
	podTitle    string
	items       []episodeItem
	cursor      int
	viewOffset  int
	windowSize  int
	showAll     bool
	skipped     int
	errMsg      string
	outPath     string
	outputCount int
	downloadIdx int
	downloadTo  string
	downloadSet []episodeItem
	downloadErr []string
	downloadOK  int
}

func newInteractiveModel(defaultFolder string) interactiveModel {
	urlInput := textinput.New()
	urlInput.Placeholder = "https://example.com/feed.rss"
	urlInput.Focus()
	urlInput.CharLimit = 512
	urlInput.Width = 60

	folderInput := textinput.New()
	folderInput.Placeholder = defaultFolder
	folderInput.SetValue(defaultFolder)
	folderInput.CharLimit = 1024
	folderInput.Width = 60

	model := interactiveModel{
		step:        stepURL,
		urlInput:    urlInput,
		folderInput: folderInput,
		windowSize:  10,
	}

	feedFile, feedOptions, err, exists := loadExtraFeeds(defaultFolder)
	if exists && err == nil && len(feedOptions) > 0 {
		model.step = stepFeedSelect
		model.feedFile = feedFile
		model.feedOptions = feedOptions
	} else if exists {
		model.feedFile = feedFile
		if err != nil {
			model.errMsg = fmt.Sprintf("Failed to read %s; enter URL manually.", feedFile)
		} else {
			model.errMsg = fmt.Sprintf("No valid URLs in %s; enter URL manually.", feedFile)
		}
	}

	return model
}

func (m interactiveModel) Init() tea.Cmd {
	return textinput.Blink
}

func (m interactiveModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch m.step {
	case stepURL:
		return m.updateURL(msg)
	case stepFeedSelect:
		return m.updateFeedSelect(msg)
	case stepLoading:
		return m.updateLoading(msg)
	case stepSelect:
		return m.updateSelect(msg)
	case stepFolder:
		return m.updateFolder(msg)
	case stepDownloading:
		return m.updateDownloading(msg)
	case stepDone:
		return m.updateDone(msg)
	default:
		return m, nil
	}
}

func (m interactiveModel) updateURL(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.updateWindowSize(msg)
		return m, nil
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		case "b", "esc":
			if len(m.feedOptions) > 0 {
				m.step = stepFeedSelect
				m.ensureFeedVisible()
				return m, nil
			}
		case "enter":
			url := strings.TrimSpace(m.urlInput.Value())
			if url == "" {
				m.errMsg = "Enter an RSS feed URL."
				return m, nil
			}

			m.errMsg = ""
			m.step = stepLoading
			return m, fetchFeedCmd(url)
		}
	}

	var cmd tea.Cmd
	m.urlInput, cmd = m.urlInput.Update(msg)
	return m, cmd
}

func (m interactiveModel) updateFeedSelect(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.updateWindowSize(msg)
		return m, nil
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		case "up", "k":
			if m.feedCursor > 0 {
				m.feedCursor--
				m.ensureFeedVisible()
			}
		case "down", "j":
			if m.feedCursor < len(m.feedOptions)-1 {
				m.feedCursor++
				m.ensureFeedVisible()
			}
		case "enter":
			if len(m.feedOptions) == 0 {
				m.errMsg = "No feeds available."
				return m, nil
			}
			url := strings.TrimSpace(m.feedOptions[m.feedCursor])
			if url == "" {
				m.errMsg = "Selected feed is empty."
				return m, nil
			}
			m.errMsg = ""
			m.step = stepLoading
			return m, fetchFeedCmd(url)
		case "m":
			m.step = stepURL
			m.urlInput.Focus()
			m.errMsg = ""
			return m, nil
		}
	}

	return m, nil
}

func (m interactiveModel) updateLoading(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.updateWindowSize(msg)
		return m, nil
	case feedParsedMsg:
		if msg.err != nil {
			m.errMsg = fmt.Sprintf("Failed to parse feed: %v", msg.err)
			m.step = stepURL
			return m, nil
		}

		m.podTitle = msg.podTitle
		m.items = msg.episodes
		m.skipped = msg.skipped
		m.cursor = 0
		m.viewOffset = 0
		m.showAll = false
		m.errMsg = ""

		if len(m.items) == 0 {
			m.errMsg = "No downloadable episodes found."
		}
		m.step = stepSelect
		return m, nil
	}

	return m, nil
}

func (m interactiveModel) updateSelect(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.updateWindowSize(msg)
		return m, nil
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
				m.ensureCursorVisible()
			}
		case "down", "j":
			if limit := m.listLimit(); limit > 0 && m.cursor < limit-1 {
				m.cursor++
				m.ensureCursorVisible()
			}
		case " ":
			if len(m.items) > 0 {
				m.items[m.cursor].selected = !m.items[m.cursor].selected
			}
		case "enter":
			if len(m.items) == 0 {
				m.errMsg = "Nothing to select."
				return m, nil
			}
			if m.selectedCount() == 0 {
				m.errMsg = "Select at least one episode."
				return m, nil
			}

			m.errMsg = ""
			m.step = stepFolder
			m.folderInput.Focus()
			return m, nil
		case "b", "esc":
			m.step = stepURL
			m.urlInput.Focus()
			return m, nil
		case "a":
			if len(m.items) > initialListLimit {
				m.showAll = !m.showAll
				m.ensureCursorVisible()
			}
		}
	}

	return m, nil
}

func (m interactiveModel) updateFolder(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.updateWindowSize(msg)
		return m, nil
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		case "enter":
			folder := strings.TrimSpace(m.folderInput.Value())
			if folder == "" {
				m.errMsg = "Destination folder is required."
				return m, nil
			}

			folder = expandHome(folder)
			info, err := os.Stat(folder)
			if err != nil || !info.IsDir() {
				m.errMsg = "Destination folder does not exist."
				return m, nil
			}

			m.errMsg = ""
			m.step = stepDownloading
			m.downloadTo = folder
			m.downloadSet = m.selectedItems()
			m.downloadIdx = 0
			m.downloadOK = 0
			m.downloadErr = nil
			if len(m.downloadSet) == 0 {
				m.errMsg = "Select at least one episode."
				m.step = stepSelect
				return m, nil
			}

			return m, downloadEpisodeCmd(folder, m.podTitle, m.downloadSet[0], 0)
		}
	}

	var cmd tea.Cmd
	m.folderInput, cmd = m.folderInput.Update(msg)
	return m, cmd
}

func (m interactiveModel) updateDownloading(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.updateWindowSize(msg)
		return m, nil
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		}
	case downloadResultMsg:
		if msg.err != nil {
			m.downloadErr = append(m.downloadErr, fmt.Sprintf("%s: %v", msg.filename, msg.err))
		} else {
			m.downloadOK++
		}

		m.downloadIdx++
		if m.downloadIdx >= len(m.downloadSet) {
			m.outputCount = m.downloadOK
			m.outPath = m.downloadTo
			m.step = stepDone
			return m, nil
		}

		return m, downloadEpisodeCmd(m.downloadTo, m.podTitle, m.downloadSet[m.downloadIdx], m.downloadIdx)
	}

	return m, nil
}

func (m interactiveModel) updateDone(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.updateWindowSize(msg)
		return m, nil
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q", "enter":
			return m, tea.Quit
		}
	}
	return m, nil
}

func (m interactiveModel) View() string {
	switch m.step {
	case stepFeedSelect:
		return m.viewFeedSelect()
	case stepURL:
		return m.viewURL()
	case stepLoading:
		return m.viewLoading()
	case stepSelect:
		return m.viewSelect()
	case stepFolder:
		return m.viewFolder()
	case stepDownloading:
		return m.viewDownloading()
	case stepDone:
		return m.viewDone()
	default:
		return ""
	}
}

func (m interactiveModel) viewURL() string {
	var b strings.Builder
	b.WriteString("Interactive mode\n\n")
	b.WriteString("RSS feed URL:\n")
	b.WriteString(m.urlInput.View())
	b.WriteString("\n")
	if m.errMsg != "" {
		b.WriteString("\n")
		b.WriteString(m.errMsg)
		b.WriteString("\n")
	}
	if len(m.feedOptions) > 0 {
		b.WriteString("\nPress b to choose from gopodder-extra.conf.\n")
	}
	b.WriteString("\nPress Enter to continue.\n")
	return b.String()
}

func (m interactiveModel) viewFeedSelect() string {
	var b strings.Builder
	b.WriteString("Select a feed (gopodder-extra.conf)\n")
	if m.feedFile != "" {
		b.WriteString(m.feedFile)
		b.WriteString("\n")
	}
	b.WriteString("\n")

	if m.errMsg != "" {
		b.WriteString(m.errMsg)
		b.WriteString("\n\n")
	}

	if len(m.feedOptions) == 0 {
		b.WriteString("No feeds available. Press m to enter a URL manually.\n")
		return b.String()
	}

	start, end := m.feedVisibleRange()
	for i := start; i < end; i++ {
		feed := m.feedOptions[i]
		cursor := " "
		if i == m.feedCursor {
			cursor = ">"
		}
		b.WriteString(fmt.Sprintf("%s %s\n", cursor, feed))
	}

	b.WriteString("\nEnter: select  m: manual URL  q: quit\n")
	return b.String()
}

func (m interactiveModel) viewLoading() string {
	return "Loading feed...\n"
}

func (m interactiveModel) viewSelect() string {
	var b strings.Builder
	b.WriteString("Select episodes (most recent first)\n")
	if m.podTitle != "" {
		b.WriteString("Podcast: ")
		b.WriteString(m.podTitle)
		b.WriteString("\n")
	}
	b.WriteString("\n")

	if m.errMsg != "" {
		b.WriteString(m.errMsg)
		b.WriteString("\n\n")
	}

	if len(m.items) == 0 {
		b.WriteString("No items available. Press b to change URL or q to quit.\n")
		return b.String()
	}

	start, end := m.visibleRange()
	for i := start; i < end; i++ {
		item := m.items[i]
		cursor := " "
		if i == m.cursor {
			cursor = ">"
		}
		check := " "
		if item.selected {
			check = "x"
		}
		b.WriteString(fmt.Sprintf("%s [%s] %s %s\n", cursor, check, item.dateStr, item.title))
	}

	b.WriteString("\nSpace: select  Enter: continue  b: back  q: quit")
	if len(m.items) > initialListLimit {
		b.WriteString("  a: toggle full list")
	}
	b.WriteString("\n")
	if m.skipped > 0 {
		b.WriteString(fmt.Sprintf("Skipped %d items without downloadable audio.\n", m.skipped))
	}
	if len(m.items) > initialListLimit {
		if m.showAll {
			b.WriteString(fmt.Sprintf("Showing all %d episodes.\n", len(m.items)))
		} else {
			b.WriteString(fmt.Sprintf("Showing %d of %d episodes.\n", initialListLimit, len(m.items)))
		}
	}

	return b.String()
}

func (m *interactiveModel) updateWindowSize(msg tea.WindowSizeMsg) {
	// Leave space for header + footer lines in select view.
	usable := msg.Height - 8
	if usable < 5 {
		usable = 5
	}
	m.windowSize = usable
	if m.step == stepSelect {
		m.ensureCursorVisible()
	} else if m.step == stepFeedSelect {
		m.ensureFeedVisible()
	}
}

func (m *interactiveModel) ensureCursorVisible() {
	limit := m.listLimit()
	if limit == 0 {
		m.cursor = 0
		m.viewOffset = 0
		return
	}
	if m.windowSize <= 0 {
		m.windowSize = 10
	}
	if m.cursor >= limit {
		m.cursor = limit - 1
	}
	if m.cursor < m.viewOffset {
		m.viewOffset = m.cursor
		return
	}
	if m.cursor >= m.viewOffset+m.windowSize {
		m.viewOffset = m.cursor - m.windowSize + 1
	}
	if m.viewOffset < 0 {
		m.viewOffset = 0
	}
	if maxOffset := limit - m.windowSize; maxOffset > 0 && m.viewOffset > maxOffset {
		m.viewOffset = maxOffset
	}
}

func (m interactiveModel) visibleRange() (int, int) {
	limit := m.listLimit()
	if m.windowSize <= 0 || m.windowSize >= limit {
		return 0, limit
	}
	start := m.viewOffset
	if start < 0 {
		start = 0
	}
	if maxOffset := limit - m.windowSize; maxOffset > 0 && start > maxOffset {
		start = maxOffset
	}
	end := start + m.windowSize
	if end > limit {
		end = limit
	}
	return start, end
}

func (m *interactiveModel) ensureFeedVisible() {
	total := len(m.feedOptions)
	if total == 0 {
		m.feedCursor = 0
		m.feedOffset = 0
		return
	}
	if m.windowSize <= 0 {
		m.windowSize = 10
	}
	if m.feedCursor >= total {
		m.feedCursor = total - 1
	}
	if m.feedCursor < m.feedOffset {
		m.feedOffset = m.feedCursor
		return
	}
	if m.feedCursor >= m.feedOffset+m.windowSize {
		m.feedOffset = m.feedCursor - m.windowSize + 1
	}
	if m.feedOffset < 0 {
		m.feedOffset = 0
	}
	if maxOffset := total - m.windowSize; maxOffset > 0 && m.feedOffset > maxOffset {
		m.feedOffset = maxOffset
	}
}

func (m interactiveModel) feedVisibleRange() (int, int) {
	total := len(m.feedOptions)
	if m.windowSize <= 0 || m.windowSize >= total {
		return 0, total
	}
	start := m.feedOffset
	if start < 0 {
		start = 0
	}
	if maxOffset := total - m.windowSize; maxOffset > 0 && start > maxOffset {
		start = maxOffset
	}
	end := start + m.windowSize
	if end > total {
		end = total
	}
	return start, end
}

func (m interactiveModel) listLimit() int {
	if m.showAll || len(m.items) <= initialListLimit {
		return len(m.items)
	}
	return initialListLimit
}

func (m interactiveModel) viewFolder() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Selected %d episodes.\n\n", m.selectedCount()))
	b.WriteString("Destination folder:\n")
	b.WriteString(m.folderInput.View())
	b.WriteString("\n")
	if m.errMsg != "" {
		b.WriteString("\n")
		b.WriteString(m.errMsg)
		b.WriteString("\n")
	}
	b.WriteString("\nPress Enter to start downloads.\n")
	return b.String()
}

func (m interactiveModel) viewDownloading() string {
	var b strings.Builder
	total := len(m.downloadSet)
	if total == 0 {
		return "No episodes selected.\n"
	}
	current := m.downloadIdx + 1
	if current > total {
		current = total
	}
	item := m.downloadSet[current-1]
	b.WriteString(fmt.Sprintf("Downloading %d of %d\n\n", current, total))
	b.WriteString(item.title)
	b.WriteString("\n")
	if len(m.downloadErr) > 0 {
		b.WriteString("\nErrors so far:\n")
		for _, msg := range m.downloadErr {
			b.WriteString(msg)
			b.WriteString("\n")
		}
	}
	return b.String()
}

func (m interactiveModel) viewDone() string {
	var b strings.Builder
	b.WriteString("Done.\n\n")
	total := len(m.downloadSet)
	b.WriteString(fmt.Sprintf("Downloaded %d of %d episodes to %s\n", m.outputCount, total, m.outPath))
	if len(m.downloadErr) > 0 {
		b.WriteString("\nErrors:\n")
		for _, msg := range m.downloadErr {
			b.WriteString(msg)
			b.WriteString("\n")
		}
	}
	b.WriteString("\nPress Enter or q to quit.\n")
	return b.String()
}

func (m interactiveModel) selectedCount() int {
	count := 0
	for _, item := range m.items {
		if item.selected {
			count++
		}
	}
	return count
}

func (m interactiveModel) selectedItems() []episodeItem {
	items := make([]episodeItem, 0, m.selectedCount())
	for _, item := range m.items {
		if item.selected {
			items = append(items, item)
		}
	}
	return items
}

func fetchFeedCmd(url string) tea.Cmd {
	return func() tea.Msg {
		pod, episodes, err := parseFeed(url)
		if err != nil {
			return feedParsedMsg{err: err}
		}

		items, skipped := buildEpisodeItems(pod, episodes)
		return feedParsedMsg{
			podTitle: pod[title],
			episodes: items,
			skipped:  skipped,
			err:      nil,
		}
	}
}

func buildEpisodeItems(pod map[string]string, episodes []M) ([]episodeItem, int) {
	podTitle := strings.TrimSpace(pod[title])
	items := make([]episodeItem, 0, len(episodes))
	skipped := 0
	now := time.Now()

	for _, ep := range episodes {
		fileURL := strings.TrimSpace(getMapString(ep, file))
		if fileURL == "" {
			skipped++
			continue
		}

		name := strings.TrimSpace(getMapString(ep, title))
		pub := strings.TrimSpace(getMapString(ep, published))
		upd := strings.TrimSpace(getMapString(ep, updated))
		timestamp := episodeTimestamp(pub, upd, now)
		dateStr := timestamp.Format("2006-01-02")

		filename := buildEpisodeFilename(podTitle, name, dateStr)
		items = append(items, episodeItem{
			title:    name,
			date:     timestamp,
			dateStr:  dateStr,
			url:      fileURL,
			filename: filename,
			selected: false,
		})
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].date.After(items[j].date)
	})

	return items, skipped
}

func buildEpisodeFilename(podcastTitle, episodeTitle, dateStr string) string {
	podcastTitle = strings.TrimSpace(podcastTitle)
	episodeTitle = strings.TrimSpace(episodeTitle)

	podcastHash := fmt.Sprintf("%x", md5.Sum([]byte(podcastTitle+episodeTitle)))
	transformedPodcastTitle := titleTransformation(podcastTitle)
	transformedTitle := titleTransformation(episodeTitle)

	return fmt.Sprintf("%s-%s-%s-%s.%s", transformedPodcastTitle, dateStr, transformedTitle, podcastHash, mp3)
}

func episodeTimestamp(publishedStr, updatedStr string, fallback time.Time) time.Time {
	if publishedStr != "" {
		if t, err := time.Parse(time.RFC3339, publishedStr); err == nil {
			return t
		}
	}
	if updatedStr != "" {
		if t, err := time.Parse(time.RFC3339, updatedStr); err == nil {
			return t
		}
	}
	return fallback
}

func getMapString(m M, key string) string {
	val, ok := m[key]
	if !ok || val == nil {
		return ""
	}
	str, ok := val.(string)
	if !ok {
		return ""
	}
	return str
}

func downloadEpisodeCmd(folder, podTitle string, item episodeItem, index int) tea.Cmd {
	return func() tea.Msg {
		filename := filepath.Join(folder, item.filename)
		cmd := exec.Command(
			"wget",
			"--no-clobber",
			"--continue",
			"--no-check-certificate",
			"--no-verbose",
			item.url,
			"-O",
			filename,
		)
		output, err := cmd.CombinedOutput()
		if err == nil {
			err = os.Chmod(filename, 0666)
		}
		if err == nil {
			tagSinglePod(filename, item.title, podTitle)
		}
		if err != nil {
			trimmed := strings.TrimSpace(string(output))
			if trimmed != "" {
				err = fmt.Errorf("%w\n%s", err, trimmed)
			}
		}
		return downloadResultMsg{index: index, filename: filename, err: err}
	}
}

// expandHome only expands a leading "~" or "~/" and leaves other paths untouched.
// Example: "/home/mike/~/tmp" is returned unchanged.
func expandHome(path string) string {
	if path == "" {
		return path
	}
	if path[0] != '~' {
		return path
	}
	if path == "~" || strings.HasPrefix(path, "~/") {
		home := os.Getenv("HOME")
		if home == "" {
			var err error
			home, err = os.UserHomeDir()
			if err != nil {
				return path
			}
		}
		if path == "~" {
			return home
		}
		return filepath.Join(home, path[2:])
	}
	return path
}

func loadExtraFeeds(baseDir string) (string, []string, error, bool) {
	path := filepath.Join(baseDir, extraConfName)
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return path, nil, nil, false
		}
		return path, nil, err, true
	}
	if info.IsDir() {
		return path, nil, fmt.Errorf("%s is a directory", path), true
	}
	urls, err := readConfig(path)
	return path, urls, err, true
}
