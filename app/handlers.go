package main

import (
	"slices"
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
		response = convertToRESPString(val)
	} else {
		response = ERR
		if isExpired {
			deleteKey(tokens[1])
		}
	}

	return response
}

func handleRPUSH(tokens []string) string {
	if len(tokens) < 3 {
		return ERR
	}

	key := tokens[1]
	val := tokens[2:]

	count := pushToList(key, val)

	return convertToRESPInt(count)
}

func handleLPUSH(tokens []string) string {
	if len(tokens) < 3 {
		return ERR
	}

	key := tokens[1]
	val := tokens[2:]
	slices.Reverse(val)
	count := prependToList(key, val)

	return convertToRESPInt(count)
}

func handleLRANGE(tokens []string) string {
	if len(tokens) < 4 {
		return ERR
	}

	start, err := strconv.Atoi(tokens[2])
	if err != nil {
		return ERR
	}
	end, err := strconv.Atoi(tokens[3])
	if err != nil {
		return ERR
	}

	reqList := getItemsFromList(tokens[1], start, end)
	return convertToRESPArray(reqList)
}

func handleLLEN(tokens []string) string {
	if len(tokens) < 2 {
		return ERR
	}

	size := getLength(tokens[1])

	return convertToRESPInt(size)
}

func handleLPOP(tokens []string) string {
	if len(tokens) < 2 {
		return ERR
	}

	var count = 1
	if len(tokens) > 2 {
		var err error
		count, err = strconv.Atoi(tokens[2])
		if err != nil {
			count = 1
		}
	}

	val, ok := popFromLeftofArray(tokens[1], count)

	if ok {
		if count == 1 {
			return convertToRESPString(val[0])
		} else {
			return convertToRESPArray(val)
		}
	} else {
		return ERR
	}
}

func handleBLPOP(tokens []string) string {
	if len(tokens) < 2 {
		return ERR
	}

	timeout := 0.0
	if len(tokens) >= 3 {
		parsedTime, err := strconv.ParseFloat(tokens[2], 64)
		if err == nil {
			timeout = parsedTime
		}
	}

	val, ok := blockingLPop(tokens[1], timeout)
	res := make([]string, 2)
	res[0] = tokens[1]
	res[1] = val
	if ok {
		return convertToRESPArray(res)
	} else {
		return NULL_ARRAY
	}
}

func handleTYPE(tokens []string) string {
	if len(tokens) < 2 {
		return ERR
	}

	val := getType(tokens[1])

	return convertToSimpleString(val)
}
