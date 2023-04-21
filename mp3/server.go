package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

var(
	serverMap map[string]string   //all other servers mapping  name -> ip:port except itself
	serverName map[int]string
	myname string
	clients map[string]Client
)

type Client struct{
	addr string
	conn net.Conn
	tx sync.Mutex
	trans_list map[int]*transaction
	commit_num int
	aborted bool
	servertotalk []server
}

type server struct{
	name string
	conn net.Conn
}

type transaction struct{
	account string
	amount int
	operation string
}

//parse the config file and return the ip and port of the server
//store the name and ip:port of other servers in serverMap
func parse_args(name string, config string) (string, string) {
	var i int
	file, err := os.Open(config)
	if err != nil {
		fmt.Println("file does not exist")
		os.Exit(1)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		info := strings.Split(scanner.Text(), " ")
		serverName[i] = info[0]
		if info[0] == name {
			return info[1], info[2]
		}else{
			serverMap[info[0]] = info[1] + ":" + info[2]
		}
	}
	return "", ""
}

// handle the message from other servers
func handleServer(conn net.Conn, servername string){

	reader := bufio.NewReader(conn)
	for{
		msg, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error: cannot read from server")
			os.Exit(1)
		}
		// need to handle all possible reply from other coordinators(serevrs)
		// ABORT -> need to tell other servers and roll back
		// COMMIT OK -> need to tell other servers and commit
		// NOT FOUND, ABORTED -> need to tell other servers and roll back
		// bunch of actual commands sent from other servers DEPOSIT, WITHDRAW, BALANCE, COMMIT, ABORT.
//		
//	standard reply format for meaningful cmds : ABORT/COMMIT ... + clientaddr	

		// some reply will not involve any account, just need to tell the client
		// OK is a special case as that means previous operation succeed, others means this client can terminate
		msglist := strings.Split(msg, " ")
		if msglist[0] == "ABORT" || msglist[0] == "COMMIT" || msglist[0] == "NOT" || msglist[0] == "OK"{

		}
	}
}

func dealclients(conn net.Conn, clientaddr string){
	reader := bufio.NewReader(conn)
	for{
		cmd, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error: cannot read from client")
			os.Exit(1)
		}
		
		client := clients[clientaddr]
		//need to handle all possible cmd from client
		//DEPOSIT, WITHDRAW, BALANCE(query), COMMIT, ABORT
		if cmd == "ABORT\n"{
			//need to tell other servers and roll back
			//use lock to make sure only one process can modify the status of this transaction:
			//potention state: now is communicating with other servers for processing this transaction and ABORT cmd is received, must 
			//wait for the result of the processing and then roll back
			client.tx.Lock()
			abort_transaction(clientaddr)
			client.tx.Unlock()
		}
		if cmd == "COMMIT\n"{
			// wait for all other servers send back OK then try to commit
			client.tx.Lock()
			commit_transaction(clientaddr)
			client.tx.Unlock()
		}
		// withdraw, deposit contains read/write operation, balance only contains read operation
		command := strings.Split(cmd, " ")
		if command[0] == "WITHDRAW\n" || command[0] == "DEPOSIT\n"{
			//parse the arguments first
			str := strings.Split(command[1], ".")
			amount, _ :=  strconv.Atoi(str[1])
			if command[0] == "WITHDRAW\n"{
				amount = -amount
			}
			// send this command to the server who owns the account
			cmd = command[0] + " " + command[1] + " " + strconv.Itoa(amount) + "\n"
			//need to tell the server involved in this command to process this transaction
			client.tx.Lock()
			applychanges(cmd, clientaddr)
			client.tx.Unlock()
			// this function is only involved in communications between servers, so its handler should be in handleServer
		}else if  command[0] == "BALANCE\n"{
			// str_tmp := strings.Split(command[1], ".")
			client.tx.Lock()
			ask_balance(command, clientaddr)
			client.tx.Unlock()
			// this function is only involved in communications between servers, so its handler should be in handleServer


		}else{
			fmt.Println("Error: invalid command")
			os.Exit(1)
		}
	}
}



func main(){
	argv := os.Args
	if len(argv) != 3 {
		fmt.Println("Usage: ./server <server_name> <config_file>")
		os.Exit(1)
	}
	servername := argv[1]
	myname = servername
	serverIP, serverPort := parse_args(servername, argv[2])
	if serverIP == "" || serverPort == "" {
		fmt.Println("No such server")
		os.Exit(1)
	}
	//k is the name of server we are going to connect to while v is its ip:port
	for k, v := range serverMap {
		conn, err := net.Dial("tcp", v)
		if err != nil {
			fmt.Println("Error: cannot connect to server", k)
			os.Exit(1)
		}
		defer conn.Close()
		go handleServer(conn, k)
	}
	listener, err := net.Listen("tcp", serverIP + ":" + serverPort)
	if err != nil {
		fmt.Println("Error: cannot listen to port", serverPort)
		os.Exit(1)
	}
	for{
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error: cannot accept connection")
			os.Exit(1)
		}
		addr := conn.RemoteAddr().String()
		clients[addr] = Client{addr, conn, sync.Mutex{}, make(map[int]*transaction), 0, false, []server{}}
		go dealclients(conn, addr)
	}
}

