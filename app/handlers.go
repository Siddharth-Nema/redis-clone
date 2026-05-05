package main

import (
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/models"
)

func handleSet(tokens []string) string {
	if len(tokens) < 2 {
		return ERR

	}
	stringStore.Set(tokens[1], tokens[2])
	keyStore.SetType(tokens[1], "string")
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
				stringStore.SetExpiry(tokens[1], exp)
			}
		case "PX":
			if i+1 < len(tokens) {
				durationInMilliSec, err := strconv.Atoi(tokens[i+1])
				if err != nil {
					return ERR
				}

				exp := time.Now().Add(time.Millisecond * time.Duration(durationInMilliSec))
				stringStore.SetExpiry(tokens[1], exp)
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
	val, exists := stringStore.Get(tokens[1])
	exp := stringStore.GetExpiry(tokens[1])
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
			stringStore.Delete(tokens[1])
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

	count := listStore.PushToList(key, val)
	keyStore.SetType(key, "list")

	return convertToRESPInt(count)
}

func handleLPUSH(tokens []string) string {
	if len(tokens) < 3 {
		return ERR
	}

	key := tokens[1]
	val := tokens[2:]
	slices.Reverse(val)
	count := listStore.PrependToList(key, val)
	keyStore.SetType(key, "list")

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

	reqList := listStore.GetItemsFromList(tokens[1], start, end)
	return convertToRESPArray(reqList)
}

func handleLLEN(tokens []string) string {
	if len(tokens) < 2 {
		return ERR
	}

	size := listStore.GetLength(tokens[1])

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

	val, ok := listStore.PopFromLeftOfArray(tokens[1], count)

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

	val, ok := listStore.BlockingLPop(tokens[1], timeout)
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

	val := keyStore.GetType(tokens[1])

	return convertToSimpleString(val)
}

func handleXADD(tokens []string) string {
	if len(tokens) < 3 {
		return ERR
	}

	key := tokens[1]
	entryID := tokens[2]
	values := tokens[3:]

	res, err := streamStore.AddToStream(key, entryID, values)
	if err != nil {
		return convertToSimpleError(err.Error())
	}
	keyStore.SetType(key, "stream")

	return convertToRESPString(res)
}

func handleXRANGE(tokens []string) string {
	if len(tokens) < 4 {
		return ERR
	}

	key := tokens[1]
	startingEntryID, err := models.ParseStreamIDFromString(tokens[2])
	if err != nil {
		return ERR
	}

	endingEntryID, err := models.ParseStreamIDFromString(tokens[3])
	if err != nil {
		return ERR
	}

	entries := streamStore.GetStreamEntries(key, startingEntryID, endingEntryID)
	res := models.StreamEntriesToReply(entries)

	return convertToRESPMultiArray(res)
}

func handleXREAD(tokens []string) string {
	if len(tokens) < 4 {
		return ERR
	}

	var rawStreamData []string
	var streamsToRead []models.ReadStream
	var timeout int
	timeout = -1
	for i := 1; i < len(tokens); i++ {
		if strings.ToLower(tokens[i]) == "block" {
			parsedTime, err := strconv.Atoi(tokens[i+1])
			if err != nil {
				return ERR
			}
			timeout = parsedTime
			i++
		}

		if strings.ToLower(tokens[i]) == "streams" {
			rawStreamData = tokens[i+1:]
			break
		}
	}

	if len(rawStreamData) == 0 || len(rawStreamData)%2 != 0 {
		return ERR
	}

	numOfStreams := len(rawStreamData) / 2

	for i := 0; i < numOfStreams; i++ {
		var startEntryID models.StreamEntryID
		var err error
		if rawStreamData[numOfStreams+i] == "$" {
			startEntryID = streamStore.GetLatestStreamEntry(rawStreamData[i])
		} else {
			startEntryID, err = models.ParseStreamIDFromString(rawStreamData[numOfStreams+i])
			if err != nil {
				return ERR
			}
		}

		streamsToRead = append(streamsToRead, models.ReadStream{
			StreamID:     rawStreamData[i],
			StartEntryID: startEntryID,
		})

	}

	data := streamStore.ReadStreams(streamsToRead, timeout)
	if len(data) == 0 {
		return NULL_ARRAY
	} else {
		return encodeRESP(models.StreamOutputToReply(data))
	}
}

func handleINCR(tokens []string) string {
	if len(tokens) < 2 {
		return ERR
	}

	key := tokens[1]

	val, ok := stringStore.Increment(key)

	if ok {
		return convertToRESPInt(val)
	} else {
		return convertToSimpleError("value is not an integer or out of range")
	}
}

func handleMULTI(client *models.Client) string {
	client.InMulti = true
	return convertToSimpleString("OK")
}

func queueCommands(tokens []string, client *models.Client) string {
	client.Queue = append(client.Queue, tokens)
	return convertToSimpleString("QUEUED")
}

func handleEXEC(client *models.Client) string {
	var response []string
	for _, query := range client.Queue {
		response = append(response, executeCommand(query, client))
	}
	client.InMulti = false
	client.Queue = [][]string{}

	return convertToRESPArrayFromBulkStrings(response)
}

func handleDISCARD(client *models.Client) string {
	client.InMulti = false
	client.Queue = [][]string{}

	return convertToSimpleString("OK")
}

func handleINFO() string {
	var b strings.Builder
	b.Grow(128)

	b.WriteString("role:")
	b.WriteString(server.Role)
	b.WriteString(CRLF)

	b.WriteString("master_replid:")
	b.WriteString(server.MasterReplID)
	b.WriteString(CRLF)

	b.WriteString("master_repl_offset:")
	b.WriteString(strconv.FormatInt(int64(server.GetOffset()), 10))

	return convertToRESPString(b.String())
}

func handleREPLCONF(tokens []string, client *models.Client) string {
	response := OK
	for i := 0; i < len(tokens); i++ {
		switch tokens[i] {
		case "listening-port":
			if i+1 < len(tokens) {
				client.ListeningPort = tokens[i+1]
				i++
			}
		case "GETACK":
			response = convertToRESPArray([]string{"REPLCONF", "ACK", strconv.Itoa(server.GetOffset())})
		case "ACK":
			if i+1 < len(tokens) {
				offset, err := strconv.Atoi(tokens[i+1])
				if err == nil {
					client.LastKnownOffset = offset
				}
				i++
			}
			response = ""
		}
	}

	return response
}

func handlePSYNC(client *models.Client) string {
	client.IsSlave = true
	server.AddReplica(client)

	rdbHex := emptyRDB
	rdbData, _ := hex.DecodeString(rdbHex)
	rdbResp := convertToRDBFile(string(rdbData))

	fullSyncResp := convertToSimpleString(fmt.Sprintf("FULLRESYNC %s 0", server.MasterReplID))
	return fullSyncResp + rdbResp
}

func handleWAIT(tokens []string) string {
	thresholdSlaves := 0
	timeout := 0

	if len(tokens) >= 2 {
		reqSlaves, err := strconv.Atoi(tokens[1])
		if err == nil {
			thresholdSlaves = reqSlaves
		}

	}
	if len(tokens) >= 3 {
		parsedTimeout, err := strconv.Atoi(tokens[2])
		if err == nil {
			timeout = parsedTimeout
		}
	}
	return convertToRESPInt(checkReplicationStatus(thresholdSlaves, timeout))

}

func handleCONFIG(tokens []string) string {
	requiredConfigs := tokens[2:]
	var responseArray []string
	for _, key := range requiredConfigs {
		switch key {
		case "dir":
			responseArray = append(responseArray, key)
			responseArray = append(responseArray, server.DirPath)
		case "dbfilename":
			responseArray = append(responseArray, key)
			responseArray = append(responseArray, server.DbFilename)
		}
	}

	return convertToRESPArray(responseArray)
}

func handleKEYS(tokens []string) string {
	if len(tokens) < 2 {
		return convertToSimpleError("Search Query not found")
	}

	searchQuery := tokens[1]
	data := keyStore.GetAllKeys()
	if searchQuery == "*" {
		return convertToRESPArray(data)
	}

	filteredData, err := FilterKeys(data, searchQuery)
	if err != nil {
		fmt.Println(err)
		return convertToSimpleError("Error filtering data: " + err.Error())
	}

	return convertToRESPArray(filteredData)
}

func handleSUSBCRIBE(tokens []string, client *models.Client) string {
	if len(tokens) < 2 {
		return convertToSimpleError("Channel name not provided")
	}

	channelName := tokens[1]

	client.AddChannelKeyToSubscribedList(channelName)
	channel := channelStore.Get(channelName)
	channel.AddClientAsSubscriber(client)

	return encodeRESP([]interface{}{"subscribe", channelName, len(client.SubscribedChannels)})
}
