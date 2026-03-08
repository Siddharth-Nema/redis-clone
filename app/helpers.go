package main

import (
	"crypto/rand"
	"strconv"
)

func generateRandomAlphanumericID(length int) (string, error) {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := 0; i < length; i++ {
		_, err := rand.Read(b[i : i+1])
		if err != nil {
			return "", err
		}
		b[i] = charset[int(b[i])%len(charset)]
	}
	return string(b), nil
}

func calculateRESPSize(args []string) int {
	size := 1 + len(strconv.Itoa(len(args))) + 2

	for _, arg := range args {
		argLen := len(arg)
		size += 1                         // "$"
		size += len(strconv.Itoa(argLen)) // Length of the number string
		size += 2                         // "\r\n"
		size += argLen                    // The actual content
		size += 2                         // Final "\r\n"
	}

	return size
}
