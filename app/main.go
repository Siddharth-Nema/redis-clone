package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

var _ = net.Listen
var _ = os.Exit

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
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
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		tokens, err := parseRESP(reader)
		if err != nil {
			fmt.Println("Client disconnected:", err)
			return
		}

		if len(tokens) > 0 {
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
			}

			conn.Write([]byte(response))
		}
	}
}
