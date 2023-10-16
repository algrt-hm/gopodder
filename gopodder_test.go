package main

import (
	"fmt"
	"strings"
	"testing"
)

// TestCheckDependencies tests checkDependencies
// and also that the python interpreter path contains python3
func TestCheckDependencies(t *testing.T) {
	gotStatus, gotPythonPath, gotEyeD3Path := checkDependencies(true)
	wantStatus := true

	if gotStatus != wantStatus || !strings.Contains(gotPythonPath, "python3") || len(gotEyeD3Path) == 0 {
		t.Errorf("got status %+v, wanted status %+v", gotStatus, wantStatus)
		t.Errorf("got python path %+v, got eyeD3 path %+v", gotPythonPath, gotEyeD3Path)
	}
}

// TestCleanText tests cleanText with a couple of strings with less usual characters
func TestCleanText(t *testing.T) {
	s := "How €200bn of 'dirty money' flowed through a Danish bank Album: Behind the Money Genre: Podcast"
	want := "How e200bn of 'dirty money' flowed through a Danish bank Album: Behind the Money Genre: Podcast"
	got := cleanText(s, 1000)

	if got != want {
		t.Errorf("got %q, wanted %q", got, want)
	}

	s = "Is Turkey about to see the end of the Erdoğan era?"
	want = "Is Turkey about to see the end of the Erdogan era?"
	got = cleanText(s, 1000)

	if got != want {
		t.Errorf("got %q, wanted %q", got, want)
	}
}

func TestIsHttpError(t *testing.T) {
	err := fmt.Errorf("http error: 403 Forbidden")
	got := isHttpError(err)
	want := true
	if got != want {
		t.Errorf("got %v, wanted %v", got, want)
	}

	err = fmt.Errorf("http error: 500 Internal Server Error")
	got = isHttpError(err)
	want = true
	if got != want {
		t.Errorf("got %v, wanted %v", got, want)
	}

	err = fmt.Errorf("some random error")
	got = isHttpError(err)
	want = false
	if got != want {
		t.Errorf("got %v, wanted %v", got, want)
	}
}
