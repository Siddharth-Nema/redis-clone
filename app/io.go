package main

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

var OK = "+OK\r\n"
var ERR = "$-1\r\n"

func convertToRESP(arg string) string {
	var response = "$"
	response += strconv.Itoa(len(arg)) + "\r\n"
	response += arg + "\r\n"

	return response
}

// parseRESP reads and parses a complete RESP array command
func parseRESP(reader *bufio.Reader) ([]string, error) {
	// Read first byte to determine type
	firstByte, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}

	if firstByte != '*' {
		return nil, fmt.Errorf("expected array marker '*'")
	}

	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	count, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil {
		return nil, err
	}

	var result []string

	// Read each bulk string in the array
	for i := 0; i < count; i++ {
		// Read bulk string marker '$'
		marker, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}

		if marker != '$' {
			return nil, fmt.Errorf("expected bulk string marker '$'")
		}

		// Read length
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}

		length, err := strconv.Atoi(strings.TrimSpace(line))
		if err != nil {
			return nil, err
		}

		// Read the actual data
		data := make([]byte, length)
		_, err = reader.Read(data)
		if err != nil {
			return nil, err
		}

		result = append(result, string(data))

		// Read trailing \r\n
		reader.ReadString('\n')
	}

	return result, nil
}
