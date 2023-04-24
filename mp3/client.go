package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"
)

var (
	recieveOK bool
)

func parse_args(config string) (string, string, string) {
	// Parse the config file
	// Return the client ip, port, and servername
	file, err := os.Open(config)
	if err != nil {
		fmt.Println("file does not exist")
		os.Exit(1)
	}
	defer file.Close()
	// Seed the random number generator with the current time
	// input e.g.: server name, ip, port
	rand.Seed(time.Now().UnixNano())
	randserver := rand.Intn(5) + 1
	scanner := bufio.NewScanner(file)
	for i := 0; i < randserver; i++ {
		if i == randserver-1 {
			scanner.Scan()
			info := strings.Split(scanner.Text(), " ")
			//return server ip, port, servername
			return info[1], info[2], info[0]
		} else {
			scanner.Scan()
			continue
		}
	}
	return "", "", ""
}

func handleClient(conn net.Conn) {
	// Continuously receive messages from the server
	reader := bufio.NewReader(conn)
	for {
		// Read the message from the server
		message, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error: cannot read from server")
			os.Exit(1)
		}
		// Print the message from the server
		fmt.Println("recieve check", message)
		if message != ""{
			recieveOK = true
		}
		fmt.Fprintf(os.Stdout, message)
		// Exit client if the message is "COMMIT OK" or "ABORTED"
		if message == "COMMIT OK\n" || message == "ABORTED\n" || message == "NOT FOUND, ABORTED\n"{
			// fmt.Fprintf(os.Stdout, message)
			os.Exit(0)
		}
	}
}

// example input "./client abcd config.txt"
func main() {
	argv := os.Args
	if len(argv) != 3 {
		fmt.Println("Usage: ./client <client_name> <config_file>")
		os.Exit(1)
	}
	fmt.Println("Client name: ", argv[1], "Config file: ", argv[2])
	//get client information from config file by random choose server to communicate with
	client_ip, client_port, servername := parse_args(argv[2])
	fmt.Println("Client ip:", client_ip, "Client port:", client_port, "Server name:", servername)
	if client_ip == "" || client_port == "" || servername == "" {
		fmt.Println("Error: invalid config file")
		os.Exit(1)
	}
	// get input from stdIn
	stdInreader := bufio.NewReader(os.Stdin)
	for {
		cmd, err := stdInreader.ReadString('\n')
		if err != nil {
			fmt.Println("Error: cannot read from stdin")
			os.Exit(1)
		}
		if cmd == "BEGIN\n" {
			fmt.Fprintf(os.Stdout, "OK\n")
			break
		}
	}
	// Connect to the server
	conn, err := net.Dial("tcp", client_ip+":"+client_port)
	if err != nil {
		fmt.Println("Error: cannot connect to server ", servername)
		os.Exit(1)
	}
	defer conn.Close()

	go handleClient(conn)
	// keep sending message to the server.
	for {
		cmd, err := stdInreader.ReadString('\n')
		if err != nil {
			fmt.Println("Error: cannot read from stdin")
			os.Exit(1)
		}
		// Send the command to the server
		fmt.Println("sending check", cmd)
		_, connerr := fmt.Fprintf(conn, cmd)
		if connerr != nil {
			fmt.Println("Error: cannot send command to server")
			os.Exit(1)
		}
		for {
			if recieveOK {
				recieveOK = false
				break
			}
		}
	}
}
