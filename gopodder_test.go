package main

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func lastestPodsFromDb() int {
	dbFileName := "/titanium/new_podcasts/gopodder.db"
	db, err := sql.Open("sqlite3", dbFileName)
	checkErr(err)

	if err == nil {
		fmt.Printf("Connected to db %s\n", dbFileName)
		defer db.Close()
	}

	query := "select filename from downloads order by tagged_at desc limit 5"

	rows, err := db.Query(query)
	checkErr(err)

	var count int
	for rows.Next() {
		err = rows.Scan(&count)
		checkErr(err)
		count += 1
	}

	return count
}

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

func TestLastestPodsFromDb(t *testing.T) {
	want := 5
	got := lastestPodsFromDb()

	if got != want {
		t.Errorf("got %q, wanted %q", got, want)
	}
}
