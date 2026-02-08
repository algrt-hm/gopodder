package main

import (
	"bufio"
	"crypto/md5"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
)

/*
Note: we want the hashing in interactive mode to be the same as the hashing in the main mode

We are aiming for the filename treatment to be the same in both modes, such that the same podcast would result in the same filename in both modes.
*/

const initialListLimit = 10
const extraConfName = "gopodder-extra.conf"
const downloadOutputLimit = 12

// runInteractive launches the Bubble Tea UI and downloads selected episodes.
func runInteractive(defaultFolder string, pythonPath string, eyeD3Dir string) error {
	if log != nil {
		prev := log.Writer()
		log.SetOutput(io.Discard)
		defer log.SetOutput(prev)
	}

	model := newInteractiveModel(defaultFolder, pythonPath, eyeD3Dir)
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
	title      string
	date       time.Time
	dateStr    string
	url        string
	filename   string
	selected   bool
	downloaded bool
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

type downloadOutputMsg struct {
	line string
	done bool
}

type interactiveModel struct {
	step               interactiveStep
	urlInput           textinput.Model
	folderInput        textinput.Model
	feedFile           string
	feedOptions        []string
	feedOptionsAreURLs bool
	feedCursor         int
	feedOffset         int
	podTitle           string
	items              []episodeItem
	allItems           []episodeItem
	hideDownloaded     bool
	cursor             int
	viewOffset         int
	windowSize         int
	showAll            bool
	skipped            int
	errMsg             string
	outPath            string
	outputCount        int
	downloadIdx        int
	downloadTo         string
	downloadSet        []episodeItem
	downloadErr        []string
	downloadOK         int
	downloadOKFiles    []string
	downloadOut        []string
	downloadCh         chan string
	pythonPath         string
	eyeD3Dir           string
}

func newInteractiveModel(defaultFolder string, pythonPath string, eyeD3Dir string) interactiveModel {
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
		pythonPath:  pythonPath,
		eyeD3Dir:    eyeD3Dir,
	}

	dbTitles, err := loadPodcastTitlesFromDatabase()
	if err == nil && len(dbTitles) > 0 {
		model.step = stepFeedSelect
		model.feedFile = dbFileName
		model.feedOptions = dbTitles
		model.feedOptionsAreURLs = false
		return model
	}
	if err != nil {
		model.errMsg = fmt.Sprintf("Failed to read podcast titles from %s; enter URL manually.", dbFileName)
	}

	feedFile, feedOptions, feedErr, exists := loadExtraFeeds(defaultFolder)
	if exists && feedErr == nil && len(feedOptions) > 0 {
		model.step = stepFeedSelect
		model.feedFile = feedFile
		model.feedOptions = feedOptions
		model.feedOptionsAreURLs = true
		model.errMsg = ""
	} else if exists && model.errMsg == "" {
		model.feedFile = feedFile
		if feedErr != nil {
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
				m.errMsg = "No podcasts available."
				return m, nil
			}
			selected := strings.TrimSpace(m.feedOptions[m.feedCursor])
			if selected == "" {
				if m.feedOptionsAreURLs {
					m.errMsg = "Selected feed is empty."
				} else {
					m.errMsg = "Selected podcast title is empty."
				}
				return m, nil
			}
			m.errMsg = ""
			m.step = stepLoading
			if m.feedOptionsAreURLs {
				return m, fetchFeedCmd(selected)
			}
			return m, loadEpisodesForPodcastCmd(selected)
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
			m.errMsg = fmt.Sprintf("Failed to load episodes: %v", msg.err)
			if len(m.feedOptions) > 0 {
				m.step = stepFeedSelect
				return m, nil
			}
			m.step = stepURL
			return m, nil
		}

		m.podTitle = msg.podTitle
		m.allItems = msg.episodes
		m.skipped = msg.skipped
		m.cursor = 0
		m.viewOffset = 0
		m.showAll = false
		m.hideDownloaded = false
		m.errMsg = ""
		m.rebuildVisibleItems()

		if len(m.allItems) == 0 {
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
			m.errMsg = ""
			if m.cursor > 0 {
				m.cursor--
				m.ensureCursorVisible()
			}
		case "down", "j":
			m.errMsg = ""
			if limit := m.listLimit(); limit > 0 && m.cursor < limit-1 {
				m.cursor++
				m.ensureCursorVisible()
			}
		case " ":
			if len(m.items) > 0 {
				if m.items[m.cursor].downloaded {
					m.errMsg = "Already downloaded. Navigate or press d to dismiss."
				} else {
					m.items[m.cursor].selected = !m.items[m.cursor].selected
					m.errMsg = ""
				}
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
			if len(m.feedOptions) > 0 {
				m.step = stepFeedSelect
				m.ensureFeedVisible()
				return m, nil
			}
			m.step = stepURL
			m.urlInput.Focus()
			return m, nil
		case "a":
			if len(m.items) > initialListLimit {
				m.showAll = !m.showAll
				m.ensureCursorVisible()
			}
		case "d":
			if m.downloadedCount() > 0 {
				m.hideDownloaded = !m.hideDownloaded
				m.rebuildVisibleItems()
				m.errMsg = ""
				if m.hideDownloaded && len(m.items) == 0 {
					m.errMsg = "All episodes already downloaded. Press d to show them."
				}
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
			m.downloadOKFiles = nil
			m.downloadOut = nil
			m.downloadCh = make(chan string, 200)
			if len(m.downloadSet) == 0 {
				m.errMsg = "Select at least one episode."
				m.step = stepSelect
				return m, nil
			}

			return m, tea.Batch(
				downloadEpisodeCmd(folder, m.podTitle, m.downloadSet[0], 0, m.downloadCh, m.pythonPath, m.eyeD3Dir),
				listenForDownloadOutput(m.downloadCh),
			)
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
			m.downloadOKFiles = append(m.downloadOKFiles, msg.filename)
		}

		m.downloadIdx++
		if m.downloadIdx >= len(m.downloadSet) {
			m.outputCount = m.downloadOK
			m.outPath = m.downloadTo
			m.step = stepDone
			return m, nil
		}

		m.downloadOut = nil
		m.downloadCh = make(chan string, 200)
		return m, tea.Batch(
			downloadEpisodeCmd(m.downloadTo, m.podTitle, m.downloadSet[m.downloadIdx], m.downloadIdx, m.downloadCh, m.pythonPath, m.eyeD3Dir),
			listenForDownloadOutput(m.downloadCh),
		)
	case downloadOutputMsg:
		if msg.done {
			return m, nil
		}
		line := strings.TrimSpace(msg.line)
		if line != "" {
			m.downloadOut = append(m.downloadOut, line)
			if len(m.downloadOut) > downloadOutputLimit {
				m.downloadOut = m.downloadOut[len(m.downloadOut)-downloadOutputLimit:]
			}
		}
		if m.downloadCh != nil {
			return m, listenForDownloadOutput(m.downloadCh)
		}
		return m, nil
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
		if m.feedOptionsAreURLs {
			b.WriteString("\nPress b to choose from gopodder-extra.conf.\n")
		} else {
			b.WriteString("\nPress b to choose a podcast title from the database.\n")
		}
	}
	b.WriteString("\nPress Enter to continue.\n")
	return b.String()
}

func (m interactiveModel) viewFeedSelect() string {
	var b strings.Builder
	if m.feedOptionsAreURLs {
		b.WriteString("Select a feed (gopodder-extra.conf)\n")
		if m.feedFile != "" {
			b.WriteString(m.feedFile)
			b.WriteString("\n")
		}
	} else {
		b.WriteString("Select a podcast\n")
		b.WriteString(fmt.Sprintf("Source: %s (database)\n", dbFileName))
		if len(m.feedOptions) > 0 {
			b.WriteString(fmt.Sprintf("%d podcasts available\n", len(m.feedOptions)))
		}
	}
	b.WriteString("\n")

	if m.errMsg != "" {
		b.WriteString(m.errMsg)
		b.WriteString("\n\n")
	}

	if len(m.feedOptions) == 0 {
		b.WriteString("No podcasts available. Press m to enter a URL manually.\n")
		return b.String()
	}

	start, end := m.feedVisibleRange()
	for i := start; i < end; i++ {
		option := m.feedOptions[i]
		cursor := " "
		if i == m.feedCursor {
			cursor = ">"
		}
		b.WriteString(fmt.Sprintf("%s %s\n", cursor, option))
	}

	b.WriteString("\nEnter: select  m: manual URL  q: quit\n")
	return b.String()
}

func (m interactiveModel) viewLoading() string {
	return "Loading episodes...\n"
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
		if m.hideDownloaded && m.downloadedCount() > 0 {
			b.WriteString("All episodes already downloaded. Press d to show them.\n")
		} else {
			b.WriteString("No items available. Press b to choose another podcast or q to quit.\n")
		}
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
		dlMark := " "
		if item.downloaded {
			dlMark = "✓"
		}
		b.WriteString(fmt.Sprintf("%s [%s] %s %s %s\n", cursor, check, dlMark, item.dateStr, item.title))
	}

	b.WriteString("\nSpace: select  Enter: continue  b: back  q: quit")
	if len(m.items) > initialListLimit {
		b.WriteString("  a: toggle full list")
	}
	if m.downloadedCount() > 0 {
		if m.hideDownloaded {
			b.WriteString("  d: show downloaded")
		} else {
			b.WriteString("  d: hide downloaded")
		}
	}
	b.WriteString("\n")
	if m.skipped > 0 {
		b.WriteString(fmt.Sprintf("Skipped %d items without downloadable audio.\n", m.skipped))
	}
	if m.hideDownloaded {
		hidden := m.downloadedCount()
		if hidden == 1 {
			b.WriteString("Hiding 1 downloaded episode.\n")
		} else {
			b.WriteString(fmt.Sprintf("Hiding %d downloaded episodes.\n", hidden))
		}
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

func (m *interactiveModel) syncSelectionsToAllItems() {
	selected := make(map[string]bool, len(m.items))
	for _, item := range m.items {
		if item.selected {
			selected[item.filename] = true
		}
	}
	for i := range m.allItems {
		m.allItems[i].selected = selected[m.allItems[i].filename]
	}
}

func (m *interactiveModel) rebuildVisibleItems() {
	m.syncSelectionsToAllItems()
	m.items = make([]episodeItem, 0, len(m.allItems))
	for _, item := range m.allItems {
		if m.hideDownloaded && item.downloaded {
			continue
		}
		m.items = append(m.items, item)
	}
	if m.cursor >= len(m.items) {
		m.cursor = len(m.items) - 1
	}
	if m.cursor < 0 {
		m.cursor = 0
	}
	m.viewOffset = 0
	m.ensureCursorVisible()
}

func (m interactiveModel) downloadedCount() int {
	count := 0
	for _, item := range m.allItems {
		if item.downloaded {
			count++
		}
	}
	return count
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
	if len(m.downloadOKFiles) > 0 {
		last := m.downloadOKFiles[len(m.downloadOKFiles)-1]
		b.WriteString("\nLast downloaded:\n")
		b.WriteString(last)
		b.WriteString("\n")
	}
	if len(m.downloadOut) > 0 {
		b.WriteString("\nWget output:\n")
		for _, line := range m.downloadOut {
			b.WriteString(line)
			b.WriteString("\n")
		}
	}
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
	if len(m.downloadOKFiles) > 0 {
		b.WriteString("\nDownloaded files:\n")
		for _, file := range m.downloadOKFiles {
			b.WriteString(file)
			b.WriteString("\n")
		}
	}
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
		if err := storeParsedFeedInInteractiveTable(pod, episodes); err != nil {
			return feedParsedMsg{err: fmt.Errorf("failed to store parsed feed in database: %w", err)}
		}

		skipped := 0
		for _, ep := range episodes {
			fileURL := strings.TrimSpace(getMapString(ep, file))
			if fileURL == "" {
				skipped++
			}
		}

		items, err := loadEpisodeItemsFromDatabase(pod[title])
		if err != nil {
			return feedParsedMsg{err: err}
		}
		return feedParsedMsg{
			podTitle: pod[title],
			episodes: items,
			skipped:  skipped,
			err:      nil,
		}
	}
}

func loadEpisodesForPodcastCmd(podcastTitle string) tea.Cmd {
	return func() tea.Msg {
		items, err := loadEpisodeItemsFromDatabase(podcastTitle)
		if err != nil {
			return feedParsedMsg{err: err}
		}
		return feedParsedMsg{
			podTitle: podcastTitle,
			episodes: items,
			skipped:  0,
			err:      nil,
		}
	}
}

func loadPodcastTitlesFromDatabase() ([]string, error) {
	if _, err := os.Stat(dbFileName); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query(`
		SELECT DISTINCT podcast_title
		FROM interactive_episodes
		WHERE podcast_title IS NOT NULL AND TRIM(podcast_title) != ''
		ORDER BY LOWER(podcast_title);
	`)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "no such table") {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()

	titles := make([]string, 0)
	for rows.Next() {
		var podcastTitle string
		if err := rows.Scan(&podcastTitle); err != nil {
			return nil, err
		}
		podcastTitle = strings.TrimSpace(podcastTitle)
		if podcastTitle != "" {
			titles = append(titles, podcastTitle)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return titles, nil
}

func loadEpisodeItemsFromDatabase(podcastTitle string) ([]episodeItem, error) {
	db, err := sql.Open(sqlite3, dbFileName)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query(`
		SELECT
			COALESCE(i.title, ''),
			COALESCE(i.published, ''),
			COALESCE(e.first_seen, i.first_seen, ''),
			COALESCE(i.file, ''),
			COALESCE(i.podcastname_episodename_hash, ''),
			CASE WHEN EXISTS (
				SELECT 1 FROM downloads AS d WHERE d.hash = i.podcastname_episodename_hash
			) THEN 1 ELSE 0 END
		FROM interactive_episodes AS i
		LEFT JOIN episodes AS e
			ON e.podcastname_episodename_hash = i.podcastname_episodename_hash
		WHERE i.podcast_title = ?
			AND i.file IS NOT NULL
			AND TRIM(i.file) != ''
		ORDER BY COALESCE(i.published, e.first_seen, i.first_seen) DESC;
	`, podcastTitle)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]episodeItem, 0)
	now := time.Now()
	for rows.Next() {
		var titleStr, publishedStr, firstSeenStr, fileURL, hash string
		var downloadedInt int
		if err := rows.Scan(&titleStr, &publishedStr, &firstSeenStr, &fileURL, &hash, &downloadedInt); err != nil {
			return nil, err
		}

		titleStr = strings.TrimSpace(titleStr)
		fileURL = strings.TrimSpace(fileURL)
		if fileURL == "" {
			continue
		}

		timestamp := episodeTimestamp(strings.TrimSpace(publishedStr), strings.TrimSpace(firstSeenStr), now)
		dateStr := timestamp.Format("2006-01-02")
		filename := buildEpisodeFilenameWithHash(podcastTitle, titleStr, dateStr, strings.TrimSpace(hash))

		items = append(items, episodeItem{
			title:      titleStr,
			date:       timestamp,
			dateStr:    dateStr,
			url:        fileURL,
			filename:   filename,
			selected:   false,
			downloaded: downloadedInt == 1,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].date.After(items[j].date)
	})

	return items, nil
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
		firstSeen := strings.TrimSpace(ts)
		timestamp := episodeTimestamp(pub, firstSeen, now)
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
	podcastHash := fmt.Sprintf("%x", md5.Sum([]byte(strings.TrimSpace(podcastTitle)+strings.TrimSpace(episodeTitle))))
	return buildEpisodeFilenameWithHash(podcastTitle, episodeTitle, dateStr, podcastHash)
}

func buildEpisodeFilenameWithHash(podcastTitle, episodeTitle, dateStr, podcastHash string) string {
	podcastTitle = strings.TrimSpace(podcastTitle)
	episodeTitle = strings.TrimSpace(episodeTitle)

	if podcastHash == "" {
		podcastHash = fmt.Sprintf("%x", md5.Sum([]byte(podcastTitle+episodeTitle)))
	}

	transformedPodcastTitle := titleTransformation(podcastTitle)
	transformedTitle := titleTransformation(episodeTitle)

	return fmt.Sprintf("%s-%s-%s-%s.%s",
		transformedPodcastTitle,
		dateStr,
		transformedTitle,
		podcastHash,
		mp3,
	)
}

func episodeTimestamp(publishedStr, firstSeenStr string, fallback time.Time) time.Time {
	if publishedStr != "" {
		if t, err := time.Parse(time.RFC3339, publishedStr); err == nil {
			return t
		}
	}
	if firstSeenStr != "" {
		if t, err := time.Parse(time.RFC3339, firstSeenStr); err == nil {
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

func downloadEpisodeCmd(folder, podTitle string, item episodeItem, index int, outputCh chan string, pythonPath string, eyeD3Dir string) tea.Cmd {
	return func() tea.Msg {
		defer close(outputCh)

		filename := filepath.Join(folder, item.filename)
		if info, err := os.Stat(filename); err == nil && !info.IsDir() {
			return downloadResultMsg{index: index, filename: filename, err: fmt.Errorf("file already exists, skipping")}
		} else if err != nil && !os.IsNotExist(err) {
			return downloadResultMsg{index: index, filename: filename, err: err}
		}

		cmd := exec.Command(
			"wget",
			"--no-clobber",
			"--continue",
			"--no-check-certificate",
			"--progress=dot:giga",
			item.url,
			"-O",
			filename,
		)

		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return downloadResultMsg{index: index, filename: filename, err: err}
		}
		stderr, err := cmd.StderrPipe()
		if err != nil {
			return downloadResultMsg{index: index, filename: filename, err: err}
		}

		if err := cmd.Start(); err != nil {
			return downloadResultMsg{index: index, filename: filename, err: err}
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go scanOutputLines(stdout, outputCh, &wg)
		go scanOutputLines(stderr, outputCh, &wg)

		err = cmd.Wait()
		wg.Wait()
		if err == nil {
			err = os.Chmod(filename, 0666)
		}
		if err == nil {
			tagSinglePod(filename, item.title, podTitle, pythonPath, eyeD3Dir)
		}
		if err == nil {
			if dbErr := recordInteractiveDownload(filename); dbErr != nil {
				err = fmt.Errorf("downloaded and tagged, but failed to update downloads table: %w", dbErr)
			}
		}
		return downloadResultMsg{index: index, filename: filename, err: err}
	}
}

func listenForDownloadOutput(ch <-chan string) tea.Cmd {
	return func() tea.Msg {
		line, ok := <-ch
		if !ok {
			return downloadOutputMsg{done: true}
		}
		return downloadOutputMsg{line: line}
	}
}

func scanOutputLines(r io.Reader, ch chan<- string, wg *sync.WaitGroup) {
	defer wg.Done()
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		select {
		case ch <- line:
		default:
			// Drop output to avoid blocking the download if buffer fills.
		}
	}
	if err := scanner.Err(); err != nil {
		select {
		case ch <- fmt.Sprintf("output error: %v", err):
		default:
		}
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
