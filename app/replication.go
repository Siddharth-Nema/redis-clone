package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/models"
)

func sendCommand(conn net.Conn, command string) ([]string, error) {
	_, err := conn.Write([]byte(command))
	if err != nil {
		return nil, err
	}

	responseBuffer := make([]byte, 4096)
	n, err := conn.Read(responseBuffer)
	if err != nil {
		return nil, err
	}

	raw := string(responseBuffer[:n])
	parts := strings.Split(raw, "\r\n")

	response := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			response = append(response, p)
		}
	}

	return response, nil
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
	_, err = sendCommand(conn, pingMsg)
	if err != nil {
		return err
	}

	replConfPort := convertToRESPArray([]string{"REPLCONF", "listening-port", server.Port})
	_, err = sendCommand(conn, replConfPort)
	if err != nil {
		return err
	}

	replConfCapabilities := convertToRESPArray([]string{"REPLCONF", "capa", "psync2"})
	_, err = sendCommand(conn, replConfCapabilities)
	if err != nil {
		return err
	}

	psyncCommand := convertToRESPArray([]string{"PSYNC", "?", "-1"})
	err = propogateCommand(conn, psyncCommand)
	if err != nil {
		return err
	}

	go readReplicationStream(conn)
	return nil
}

func propogateCommandToReplicas(tokens []string) {
	command := convertToRESPArray(tokens)
	server.MasterReplOffset += calculateRESPSize(tokens)
	for _, slave := range server.GetReplicas() {
		propogateCommand(slave.Conn, command)
	}

}

func readReplicationStream(conn net.Conn) {
	reader := bufio.NewReader(conn)

	line, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("replication read FULLRESYNC error:", err)
		return
	}
	fmt.Println("FULLRESYNC response:", strings.TrimSpace(line))

	// Read $<len>\r\n<rdb-bytes>
	b, err := reader.ReadByte()
	if err != nil {
		fmt.Println("invalid RDB prefix read:", err)
		return
	}
	if b != '$' {
		fmt.Println("expected '$' for RDB length, got:", string(b))
		return
	}

	lenLine, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("replication read RDB length error:", err)
		return
	}

	rdbLen, err := strconv.Atoi(strings.TrimSpace(lenLine))
	if err != nil {
		fmt.Println("invalid RDB length:", err)
		return
	}

	if rdbLen > 0 {
		rdb := make([]byte, rdbLen)
		if _, err := io.ReadFull(reader, rdb); err != nil {
			fmt.Println("replication read RDB payload error:", err)
			return
		}
	}

	dummyClient := &models.Client{}
	for {
		tokens, bytesRead, err := parseRESP(reader)
		if err != nil {
			fmt.Println("replication stream ended:", err)
			return
		}
		if len(tokens) == 0 {
			continue
		}

		response := executeCommand(tokens, dummyClient)
		server.MasterReplOffset += bytesRead // for slave
		//fmt.Println(tokens)
		if tokens[0] == "REPLCONF" {
			conn.Write([]byte(response))
		}

	}
}

func checkReplicationStatus(thresholdSlaves int, timeoutMs int) int {
	replicas := server.GetReplicas()
	numReplicas := len(replicas)
	thresholdOffsest := server.MasterReplOffset

	if thresholdSlaves == 0 || thresholdOffsest == 0 {
		return numReplicas
	}

	propogateCommandToReplicas(strings.Split(getACKCommand, " "))

	var timeoutChan <-chan time.Time
	if timeoutMs > 0 {
		timeoutChan = time.After(time.Duration(timeoutMs) * time.Millisecond)
	}

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		satisfiedCount := 0

		replicas := server.GetReplicas()
		for _, slave := range replicas {
			if slave.LastKnownOffset >= thresholdOffsest {
				satisfiedCount++
			}
		}
		if satisfiedCount >= thresholdSlaves {

			return satisfiedCount
		}

		select {
		case <-timeoutChan:
			return satisfiedCount
		case <-ticker.C:

		}
	}
}
