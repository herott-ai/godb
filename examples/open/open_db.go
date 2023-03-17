package main

import (
	"fmt"
	"github.com/herott-ai/godb"
	"path/filepath"
)

func main() {
	path := filepath.Join("/tmp", "godb")
	// specify other options
	// opts.XXX
	opts := godb.DefaultOptions(path)
	db, err := godb.Open(opts)
	if err != nil {
		fmt.Printf("open godb err: %v", err)
		return
	}
	defer func() {
		_ = db.Close()
	}()
}
