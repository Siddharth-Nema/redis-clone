package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/models"
)

var _ = net.Listen
var _ = os.Exit

var role = "master"
var port = 6379
var replicaOf string
var master_replid string
var master_repl_offset int

func main() {
	fmt.Println("Logs from your program will appear here!")

	generatedID, err := generateRandomAlphanumericID(40)
	if err == nil {
		master_replid = generatedID
	} else {
		master_replid = defaultMasterID
	}

	master_repl_offset = 0

	for idx, arg := range os.Args {
		if arg == "--port" && len(os.Args) > idx {
			newPort, err := strconv.Atoi(os.Args[idx+1])
			if err == nil {
				port = newPort
			}
		} else if arg == "--replicaof" && len(os.Args) > idx {
			replicaOf = os.Args[idx+1]
			role = "slave"
		}

	}

	listeningAddress := "0.0.0.0:" + strconv.Itoa(port)

	l, err := net.Listen("tcp", listeningAddress)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		client := models.NewClient(conn)
		go handleConnection(client)
	}
}

func handleConnection(client *models.Client) {
	defer client.Close()
	reader := bufio.NewReader(client.Conn)

	for {
		tokens, err := parseRESP(reader)
		if err != nil {
			fmt.Println("Client disconnected:", err)
			return
		}

		if len(tokens) > 0 {
			client.Conn.Write([]byte(processCommand(tokens, client)))
		}
	}
}

func processCommand(tokens []string, client *models.Client) string {
	command := tokens[0]
	switch command {
	case "EXEC":
		if !client.InMulti {
			return convertToSimpleError("EXEC without MULTI")
		}
		return handleEXEC(client)
	case "DISCARD":
		if !client.InMulti {
			return convertToSimpleError("DISCARD without MULTI")
		}
		return handleDISCARD(client)
	case "MULTI":
		return handleMULTI(client)
	default:
		if client.InMulti {
			return queueCommands(tokens, client)
		}
		return executeCommand(tokens)
	}
}

func executeCommand(tokens []string) string {
	command := tokens[0]
	var response string
	switch command {
	case "PING":
		response = "+PONG\r\n"

	case "ECHO":
		if len(tokens) > 1 {
			arg := tokens[1]
			response = convertToRESPString(arg)
		}

	case "SET":
		response = handleSet(tokens)
	case "GET":
		response = handleGet(tokens)
	case "RPUSH":
		response = handleRPUSH(tokens)
	case "LPUSH":
		response = handleLPUSH(tokens)
	case "LRANGE":
		response = handleLRANGE(tokens)
	case "LLEN":
		response = handleLLEN(tokens)
	case "LPOP":
		response = handleLPOP(tokens)
	case "BLPOP":
		response = handleBLPOP(tokens)
	case "TYPE":
		response = handleTYPE(tokens)
	case "XADD":
		response = handleXADD(tokens)
	case "XRANGE":
		response = handleXRANGE(tokens)
	case "XREAD":
		response = handleXREAD(tokens)
	case "INCR":
		response = handleINCR(tokens)
	case "INFO":
		response = handleINFO(tokens)
	}

	return response
}
