package main

import "crypto/rand"

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
