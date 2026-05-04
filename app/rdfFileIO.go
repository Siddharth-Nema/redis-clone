package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/models"
)

func LoadDataFromRDBFile() {
	rdbFilePath := filepath.Join(server.DirPath, server.DbFilename)

	file, err := os.Open(rdbFilePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			fmt.Println("RDB file not found. Starting with an empty database.")
			return
		}
		fmt.Printf("Error opening RDB file: %v\n", err)
		return
	}
	defer file.Close()

	reader := models.NewRDBParser(file)

	data, err := reader.Parse()

	for key, value := range data {
		stringStore.Set(key, value.Val)
		if value.ExpInMs != 0 {
			stringStore.SetExpiry(key, time.UnixMilli(int64(value.ExpInMs)))
		}

		keyStore.SetType(key, "string")
	}
}
