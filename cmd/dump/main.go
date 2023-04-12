package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	splunker "splunkdump"
)

const baseDir = "./adsb_bratwurst"

func main() {
	go http.ListenAndServe(":8080", nil)

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	if err := OpenDB(baseDir); err != nil {
		log.Fatal(err)
	}
}

func OpenDB(p string) error {
	dirs := []string{
		filepath.Join(p, "db"),
		filepath.Join(p, "colddb"),
	}

	for _, dir := range dirs {
		if err := readBuckets(dir); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	return nil
}

func readBuckets(name string) error {
	dir, err := os.ReadDir(name)
	if err != nil {
		return err
	}

	for _, e := range dir {
		if !e.IsDir() {
			continue
		}

		p := filepath.Join(name, e.Name())
		decoder, err := splunker.NewJournalDecoder(p)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("NewJournalDecoder(%q): %v", p, err)
		}

		if err := runDecode(decoder); err != nil {
			return fmt.Errorf("runDecode: %q: %v", p, err)
		}
	}

	return nil
}

func runDecode(d *splunker.JournalDecoder) error {
	var e splunker.Event
	for d.Scan() {
		e = d.Event()
		_ = e
		fmt.Println(e.MessageString())
		//fmt.Println(fmt.Sprintf("%s - %s - %s: %s", d.Host(), d.SourceType(), d.Source(), e.MessageString()))
	}

	return d.Err()
}
