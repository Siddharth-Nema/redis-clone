package main

import (
	"fmt"
	"net"
)

func sendPing(address string) error {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return err
	}
	defer conn.Close()

	pingMsg := "*1\r\n$4\r\nPING\r\n"

	_, err = conn.Write([]byte(pingMsg))
	if err != nil {
		return err
	}

	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		return err
	}

	fmt.Printf("Received: %s", string(buffer[:n]))
	return nil
}
