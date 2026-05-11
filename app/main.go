package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/io"
	"github.com/codecrafters-io/redis-starter-go/app/models"
	"github.com/codecrafters-io/redis-starter-go/app/store"
)

var _ = net.Listen
var _ = os.Exit

// Instance of the current running redis server
var server *models.RedisServer

var (
	keyStore     = store.NewKeyStore()
	stringStore  = store.NewStringStore()
	listStore    = store.NewListStore()
	streamStore  = store.NewStreamStore()
	channelStore = store.NewChannelStore()
)

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
		case "--dir":
			if i+1 < len(os.Args) {
				server.DirPath = os.Args[i+1]
				i++
			}
		case "--dbfilename":
			if i+1 < len(os.Args) {
				server.DbFilename = os.Args[i+1]
				i++
			}
		}
	}

	LoadDataFromRDBFile()

	if server.Role == "slave" {
		go sendHandshakeToMaster()
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
		tokens, _, err := io.ParseRESP(reader)
		if err != nil {
			fmt.Println("Client disconnected:", err)
			return
		}

		// fmt.Println(tokens)

		if len(tokens) > 0 {
			response := processCommand(tokens, client)
			client.Send(response)
		}
	}
}

func processCommand(tokens []string, client *models.Client) string {
	command := tokens[0]

	if client.State == models.StateSubscribed {
		return handleSubscribeMode(tokens, client)
	}

	switch command {
	case "EXEC":
		if client.State != models.StateMulti {
			return io.ConvertToSimpleError("EXEC without MULTI")
		}
		return handleEXEC(client)
	case "DISCARD":
		if client.State != models.StateMulti {
			return io.ConvertToSimpleError("DISCARD without MULTI")
		}
		return handleDISCARD(client)
	case "MULTI":
		return handleMULTI(client)
	default:
		if client.State == models.StateMulti {
			return queueCommands(tokens, client)
		}
		return executeCommand(tokens, client)
	}
}

func handleSubscribeMode(tokens []string, client *models.Client) string {
	command := tokens[0]
	var response string

	switch command {
	case "PING":
		response = io.ConvertToRESPArray([]string{"pong", ""})
	case "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "QUIT":
		response = executeCommand(tokens, client)
	default:
		response = io.ConvertToSimpleError("Can't execute '" + command + "': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context")
	}

	return response
}

func executeCommand(tokens []string, client *models.Client) string {
	command := strings.ToUpper(tokens[0])
	var response string
	switch command {
	case "PING":
		response = "+PONG\r\n"

	case "ECHO":
		if len(tokens) > 1 {
			arg := tokens[1]
			response = io.ConvertToRESPString(arg)
		}

	case "SET":
		response = handleSet(tokens)
		if response != io.ERR && server.IsMaster() {
			go propogateCommandToReplicas(tokens)
		}
	case "GET":
		response = handleGet(tokens)
	case "RPUSH":
		response = handleRPUSH(tokens)
		if response != io.ERR && server.IsMaster() {
			go propogateCommandToReplicas(tokens)
		}
	case "LPUSH":
		response = handleLPUSH(tokens)
		if response != io.ERR && server.IsMaster() {
			go propogateCommandToReplicas(tokens)
		}
	case "LRANGE":
		response = handleLRANGE(tokens)
	case "LLEN":
		response = handleLLEN(tokens)
	case "LPOP":
		response = handleLPOP(tokens)
	case "BLPOP":
		response = handleBLPOP(tokens)
		if response != io.ERR && server.IsMaster() {
			go propogateCommandToReplicas(tokens)
		}
	case "TYPE":
		response = handleTYPE(tokens)
	case "XADD":
		response = handleXADD(tokens)
		if response != io.ERR && server.IsMaster() {
			go propogateCommandToReplicas(tokens)
		}
	case "XRANGE":
		response = handleXRANGE(tokens)
	case "XREAD":
		response = handleXREAD(tokens)
	case "INCR":
		response = handleINCR(tokens)
		if response != io.ERR && server.IsMaster() {
			go propogateCommandToReplicas(tokens)
		}
	case "INFO":
		response = handleINFO()
	case "REPLCONF":
		response = handleREPLCONF(tokens, client)
	case "PSYNC":
		response = handlePSYNC(client)
	case "WAIT":
		response = handleWAIT(tokens)
	case "CONFIG":
		response = handleCONFIG(tokens)
	case "KEYS":
		response = handleKEYS(tokens)
	case "SUBSCRIBE":
		response = handleSUSBCRIBE(tokens, client)
	case "PUBLISH":
		response = handlePUBLISH(tokens)
	case "UNSUBSCRIBE":
		response = handleUNSUBSCRIBE(tokens, client)
	}

	return response
}
