package main

import (
	"strconv"
	"strings"
	"time"
)

func handleSet(tokens []string) string {
	if len(tokens) < 2 {
		return ERR

	}
	set(tokens[1], tokens[2])
	for i := 3; i < len(tokens); i = i + 2 {
		flag := tokens[i]
		flag = strings.ToUpper(flag)

		switch flag {
		case "EX":
			if i+1 < len(tokens) {
				durationInSec, err := strconv.Atoi(tokens[i+1])
				if err != nil {
					return ERR
				}

				exp := time.Now().Add(time.Second * time.Duration(durationInSec))
				setExpiry(tokens[1], exp)
			}
		case "PX":
			if i+1 < len(tokens) {
				durationInMilliSec, err := strconv.Atoi(tokens[i+1])
				if err != nil {
					return ERR
				}

				exp := time.Now().Add(time.Millisecond * time.Duration(durationInMilliSec))
				setExpiry(tokens[1], exp)
			}
		}
	}

	return OK
}

func handleGet(tokens []string) string {
	var response string
	if len(tokens) < 2 {
		return ERR
	}
	val, exists := get(tokens[1])
	exp := getExpiry(tokens[1])
	var isExpired bool
	if exp.IsZero() {
		isExpired = false
	} else {
		isExpired = time.Now().After(exp)
	}
	if exists && !isExpired {
		response = convertToRESP(val)
	} else {
		response = ERR
		if isExpired {
			deleteKey(tokens[1])
		}
	}

	return response
}
