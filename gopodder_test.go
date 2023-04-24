package main

import "testing"

func TestCleanText(t *testing.T) {
	s := "How €200bn of 'dirty money' flowed through a Danish bank Album: Behind the Money Genre: Podcast"
	want := "How e200bn of 'dirty money' flowed through a Danish bank Album: Behind the Money Genre: Podcast"

	got := CleanText(s, 1000)

	if got != want {
		t.Errorf("got %q, wanted %q", got, want)
	}
}
