package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

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

func print(output chan string, doneOnboarding chan bool) {
	onboardingDone := false
	for line := range output {
		if line == "\x00READY\x00\r\n" {
			onboardingDone = true
			fmt.Print("> ")
			doneOnboarding <- true
		} else {
			fmt.Print(line)
			if onboardingDone {
				fmt.Print("> ")
			}
		}
	}
}

func main() {
	output := make(chan string)
	doneOnboarding := make(chan bool)
	client, err := net.Dial("tcp", "localhost:1234")
	if err != nil {
		fmt.Println("Couldn't connect")
		return
	}
	go read(client, output)
	go print(output, doneOnboarding)

	onboardingDone := false
	client_reader := bufio.NewReader(os.Stdin)
	for {
		text, _ := client_reader.ReadString('\n')
		client.Write([]byte(text))

		if !onboardingDone {
			select {
			case <-doneOnboarding:
				onboardingDone = true
				fmt.Print("> ")
			default:
			}
		} else {
			fmt.Print("> ")
		}
	}
}
