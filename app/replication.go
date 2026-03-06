package main

import (
	"bufio"
	"fmt"
	"net"

	"github.com/codecrafters-io/redis-starter-go/app/models"
)

func sendCommand(conn net.Conn, command string) error {
	_, err := conn.Write([]byte(command))
	if err != nil {
		return err
	}

	responseBuffer := make([]byte, 1024)
	bytesRead, err := conn.Read(responseBuffer)
	if err != nil {
		return err
	}

	fmt.Printf("Received: %s", string(responseBuffer[:bytesRead]))
	return nil
}

func propogateCommand(conn net.Conn, command string) error {
	_, err := conn.Write([]byte(command))
	if err != nil {
		return err
	}
	return nil
}

func sendHandshakeToMaster() error {
	address := server.MasterHost + ":" + server.MasterPort
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return err
	}

	pingMsg := "*1\r\n$4\r\nPING\r\n"
	err = sendCommand(conn, pingMsg)
	if err != nil {
		return err
	}

	replConfPort := convertToRESPArray([]string{"REPLCONF", "listening-port", server.Port})
	err = sendCommand(conn, replConfPort)
	if err != nil {
		return err
	}

	replConfCapabilities := convertToRESPArray([]string{"REPLCONF", "capa", "psync2"})
	err = sendCommand(conn, replConfCapabilities)
	if err != nil {
		return err
	}

	psyncCommand := convertToRESPArray([]string{"PSYNC", "?", "-1"})
	err = sendCommand(conn, psyncCommand)
	if err != nil {
		return err
	}

	go readReplicationStream(conn)
	return nil
}

func propogateCommandToReplicas(tokens []string) {
	for _, slave := range server.GetReplicas() {
		propogateCommand(slave.Conn, convertToRESPArray(tokens))
	}
}

func readReplicationStream(conn net.Conn) {
	reader := bufio.NewReader(conn)
	dummyClient := &models.Client{}
	for {
		tokens, err := parseRESP(reader)
		if err != nil {
			fmt.Println("replication stream ended:", err)
			return
		}
		if len(tokens) == 0 {
			continue
		}
		_ = executeCommand(tokens, dummyClient)
	}
}
