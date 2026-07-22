package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"sync"
)

var mu sync.Mutex

func read(conn net.Conn, output chan string) {
	reader := bufio.NewReader(conn)
	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			fmt.Print("Disconnected from server")
			return
		}
		output <- message
	}
}

func print(output chan string) {
	for line := range output {
		fmt.Print(line)
	}
}

func main() {
	output := make(chan string)
	client, err := net.Dial("tcp", "localhost:1234")
	if err != nil {
		fmt.Println("Couldn't connect")
		return
	}
	go read(client, output)
	go print(output)
	client_reader := bufio.NewReader(os.Stdin)
	for {
		text, _ := client_reader.ReadString('\n')
		client.Write([]byte(text))
		output <- ">"

	}
}
