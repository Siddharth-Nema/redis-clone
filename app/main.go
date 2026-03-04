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

func main() {
	fmt.Println("Logs from your program will appear here!")

	port := 6379

	for idx, arg := range os.Args {
		if arg == "--port" && len(os.Args) > idx {
			newPort, err := strconv.Atoi(os.Args[idx+1])
			if err == nil {
				port = newPort
			}
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
	}

	return response
}
