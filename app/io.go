package main

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

var OK = "+OK\r\n"
var ERR = "$-1\r\n"
var CRLF = "\r\n"
var NULL_ARRAY = "*-1\r\n"

func convertToSimpleString(arg string) string {
	return "+" + arg + "\r\n"
}

func convertToRESPString(arg string) string {
	var response = "$"
	response += strconv.Itoa(len(arg)) + CRLF
	response += arg + CRLF

	return response
}

func convertToRESPInt(arg int) string {
	var response = ":"
	response += strconv.Itoa(arg) + CRLF

	return response
}

func convertToRESPArray(args []string) string {
	var response = "*" + strconv.Itoa(len(args)) + CRLF
	for _, arg := range args {
		response += "$" + strconv.Itoa(len(arg)) + CRLF + arg + CRLF
	}

	return response
}

func convertToRESPMultiArray(items [][]interface{}) string {
	var response = "*" + strconv.Itoa(len(items)) + CRLF

	for _, item := range items {
		response += "*2" + CRLF

		idStr := item[0].(string)
		response += "$" + strconv.Itoa(len(idStr)) + CRLF + idStr + CRLF

		vals := item[1].([]string)
		response += "*" + strconv.Itoa(len(vals)) + CRLF
		for _, v := range vals {
			response += "$" + strconv.Itoa(len(v)) + CRLF + v + CRLF
		}
	}

	return response
}

func encodeRESP(v interface{}) string {
	switch val := v.(type) {

	case string:
		return convertToRESPString(val)

	case []string:
		resp := "*" + strconv.Itoa(len(val)) + CRLF
		for _, s := range val {
			resp += convertToRESPString(s)
		}
		return resp

	case []interface{}:
		resp := "*" + strconv.Itoa(len(val)) + CRLF
		for _, item := range val {
			resp += encodeRESP(item)
		}
		return resp

	case [][]interface{}:
		resp := "*" + strconv.Itoa(len(val)) + CRLF
		for _, item := range val {
			resp += encodeRESP(item)
		}
		return resp

	default:
		panic(fmt.Sprintf("unsupported RESP type: %T", v))
	}
}

func convertToSimpleError(args string) string {
	return "-ERR " + args + "\r\n"
}

// parseRESP reads and parses a complete RESP array command
func parseRESP(reader *bufio.Reader) ([]string, int, error) {

	var bytesRead int

	// Read first byte to determine type
	firstByte, err := reader.ReadByte()
	if err != nil {
		return nil, 0, err
	}

	if firstByte != '*' {
		return nil, 0, fmt.Errorf("expected array marker '*'")
	}

	bytesRead++ // for '*'

	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, 0, err
	}

	bytesRead += len(line) // for count line including \r\n

	count, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil {
		return nil, 0, err
	}

	var result []string

	// Read each bulk string in the array
	for i := 0; i < count; i++ {
		// Read bulk string marker '$'
		marker, err := reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}

		bytesRead++ // for '$'

		if marker != '$' {
			return nil, 0, fmt.Errorf("expected bulk string marker '$'")
		}

		// Read length
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, 0, err
		}

		bytesRead += len(line) // for length line including \r\n

		length, err := strconv.Atoi(strings.TrimSpace(line))
		if err != nil {
			return nil, 0, err
		}

		// Read the actual data
		data := make([]byte, length)
		n, err := reader.Read(data)
		if err != nil {
			return nil, 0, err
		}

		bytesRead += n // for actual data

		result = append(result, string(data))

		// Read trailing \r\n
		crlf := make([]byte, 2)
		n, err = reader.Read(crlf)
		if err != nil {
			return nil, 0, err
		}

		bytesRead += n // for \r\n
	}

	return result, bytesRead, nil
}

func convertToRESPArrayFromBulkStrings(items []string) string {
	var response = "*" + strconv.Itoa(len(items)) + CRLF

	for _, item := range items {
		response += item
	}

	return response
}

func convertToRDBFile(file string) string {
	return "$" + strconv.Itoa(len(file)) + "\r\n" + file
}
