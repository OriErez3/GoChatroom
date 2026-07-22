package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func read(conn net.Conn) {
	reader := bufio.NewReader(conn)
	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			fmt.Print("Disconnected from server")
			return
		}
		fmt.Print(message)
	}
}

func main() {
	client, err := net.Dial("tcp", "localhost:1234")
	if err != nil {
		fmt.Println("Couldn't connect")
		return
	}
	go read(client)
	client_reader := bufio.NewReader(os.Stdin)
	for {
		text, _ := client_reader.ReadString('\n')
		client.Write([]byte(text))
	}
}
