package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/models"
)

var _ = net.Listen
var _ = os.Exit

var server *models.RedisServer

func main() {
	fmt.Println("Logs from your program will appear here!")

	server = models.NewRedisServer()

	generatedID, err := generateRandomAlphanumericID(40)
	if err == nil {
		server.MasterReplID = generatedID
	} else {
		server.MasterReplID = defaultMasterID
	}

	for i := 0; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "--port":
			if i+1 < len(os.Args) {
				server.Port = os.Args[i+1]
				i++
			}
		case "--replicaof":
			if i+1 < len(os.Args) {
				parts := strings.Fields(os.Args[i+1])
				if len(parts) >= 2 {
					server.MasterHost = parts[0]
					if server.MasterHost == "localhost" {
						server.MasterHost = "127.0.0.1"
					}
					server.MasterPort = parts[1]
					server.Role = "slave"
				}
				i++
			}
		}
	}

	if server.Role == "slave" {
		go sendPing(server.MasterHost + ":" + server.MasterPort)
	}

	listeningAddress := "0.0.0.0:" + server.Port
	l, err := net.Listen("tcp", listeningAddress)
	if err != nil {
		fmt.Println("Failed to bind to port " + server.Port)
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
