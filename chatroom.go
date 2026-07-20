package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
)

func broadcast(c map[net.Conn]*Client, newClients chan *Client, disconnects chan *Client, messages chan Message) {
	for {
		select {
		case newClient := <-newClients:
			c[newClient.conn] = newClient
		case disconnect := <-disconnects:
			delete(c, disconnect.conn)
		case msg := <-messages:
			switch isaPM := msg.isPM; isaPM {
			case true:
				for con, client := range c {
					if msg.pmTarget == client.username {
						_, write_err := con.Write([]byte(msg.sender.username + ": " + msg.text))
						if write_err != nil {
							fmt.Println(write_err)
							continue

						}
					}
				}

			case false:
				for con, client := range c {
					if msg.room == client.room {
						_, write_err := con.Write([]byte(msg.sender.username + ": " + msg.text))
						if write_err != nil {
							fmt.Println(write_err)
							continue

						}
					}
				}
				rooms_list.mu.Lock()
				room := rooms_list.list[msg.room]
				rooms_list.mu.Unlock()
				room.mu.Lock()
				room.history = append(room.history, msg.text)
				room.mu.Unlock()
			}
		}
	}
}

func read(user *Client, messages chan Message, disconnects chan *Client, room string) {
	reader := bufio.NewReader(user.conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			disconnects <- user
			return
		}
		if line != "" && strings.HasPrefix(line, "/pm") {
			message_split := strings.Split(line, " ")
			messages <- Message{sender: user, room: room, text: strings.Join(message_split[2:], " "), isPM: true, pmTarget: message_split[1]}
		} else if line != "" {
			messages <- Message{sender: user, room: room, text: line}
		}
	}
}

type Client struct {
	conn     net.Conn
	username string
	room     string
}

type Room struct {
	history []string
	mu      sync.Mutex
}

type Message struct {
	sender   *Client
	room     string
	text     string
	isPM     bool
	pmTarget string
}

type rooms struct {
	list map[string]*Room
	mu   sync.Mutex
}

var rooms_list rooms

func listen(clients map[net.Conn]*Client, newClients chan *Client, disconnects chan *Client, messages chan Message) {
	listen, err := net.Listen("tcp", ":1234")
	if err != nil {
		fmt.Println(err)
		return
	}
	for {
		con, err := listen.Accept()
		if err != nil {
			continue
		}
		if con != nil {
			go onboarding(clients, con, newClients, disconnects, messages)
		}
	}
}

func onboarding(clients map[net.Conn]*Client, connection net.Conn, newClients chan *Client, disconnects chan *Client, messages chan Message) {
	reader := bufio.NewReader(connection)
	_, write_err := connection.Write([]byte("Welcome! Choose a username: "))
	if write_err != nil {
		return
	}
	user_name := ""
	for {
		line, read_err := reader.ReadString('\n')
		if line != "" {
			user_name = strings.TrimSpace(line)
			break
		}
		if read_err != nil {
			fmt.Println(read_err)
			return
		}
	}
	rooms_list.mu.Lock()
	_, _ = connection.Write([]byte("List of rooms: "))
	for room, _ := range rooms_list.list {
		_, _ = connection.Write([]byte(room + ", "))
	}
	rooms_list.mu.Unlock()
	_, write_err = connection.Write([]byte("\r\nChoose a room, or create one by typing a non-exisitent room: "))
	if write_err != nil {
		return
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			disconnects <- &Client{conn: connection}
			return
		}
		if line != "" {
			line = strings.TrimSpace(line)
			rooms_list.mu.Lock()
			if _, ok := rooms_list.list[line]; !ok {
				rooms_list.list[line] = &Room{}
			}
			rooms_list.mu.Unlock()
			newClient := &Client{conn: connection, username: user_name, room: line}
			newClients <- newClient
			_, _ = connection.Write([]byte("Active Users: "))
			for _, val := range clients {
				if val.room == newClient.room {
					_, _ = connection.Write([]byte(val.username + ", "))
				}
			}
			_, _ = connection.Write([]byte("\r\n"))
			go read(newClient, messages, disconnects, newClient.room)
			break

		}
	}
}

func main() {
	rooms_list.list = make(map[string]*Room)
	clients := make(map[net.Conn]*Client)
	newClients := make(chan *Client)
	disconnects := make(chan *Client)
	messages := make(chan Message)
	go broadcast(clients, newClients, disconnects, messages)
	go listen(clients, newClients, disconnects, messages)
	select {}

}
