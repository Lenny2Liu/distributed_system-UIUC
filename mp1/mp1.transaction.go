package main

import (
	"fmt"
	"os"
	"net"
	"strconv"
	"strings"
	"time"
	"bufio"
	"sync"
)

func real_time() float64{
	return float64(time.Now().UnixMicro())/float64(1000000)
}

func Multicast(event string, conn net.Conn){
	for i := range(node_list){
		if node_list[i].info.if_active{
			node_list[i].info.conn.Write([]byte(event))
		}
	}
}

func communicate(node_name string, node_host string, node_port int){
	conn, err := net.Dial("tcp", node_host + ":" + strconv.Itoa(node_port))
	if err != nil{
		fmt.Println("Error: ", err)
		os.Exit(1)
	}
	for i := range(node_list){
		if node_list[i].node_name == node_name{
			node_list[i].info.if_active = true
			node_list[i].info.conn = conn
		}
	}
}

func read_config(filename string) (node_num int, node_name []string, node_host[]string, node_port[]int){
	file, err := os.Open(filename)
	if err != nil{
		fmt.Println("Error: ", err)
		os.Exit(1)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Scan()
	node_num, _ = strconv.Atoi(scanner.Text())
	for scanner.Scan(){
		line_components := strings.Split(scanner.Text(), " ")
		node_name = append(node_name, line_components[0])
		node_host = append(node_host, line_components[1])
		port,_:= strconv.Atoi(line_components[2])
		node_port = append(node_port, port)
	}

	return node_num, node_name, node_host, node_port
}

func main(){
	var i int
	var speaker int
	args := os.Args
	identifier := args[1]
	node_num, node_name, node_host, node_port := read_config(args[2])
	for i < node_num{
		if(node_name[i] == identifier){
			speaker = node_port[i]
		}
		go communicate(node_name[i], node_host[i], node_port[i])
	}
	listener, err := net.Listen("tcp", ":" + strconv.Itoa(speaker))
	if err != nil{
		fmt.Println("Error: ", err)
		os.Exit(1)
	}
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil{
			fmt.Println("Error: ", err)
			os.Exit(1)
		}
		go connection_handler(conn)
	}
}

func connection_handler(conn net.Conn){
	message, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil{
		fmt.Println("Error: ", err)
		os.Exit(1)
	}
	tx.Lock()
	if message == "" || message == "\n"{
		tx.Unlock()
		return
	}
	message_list := strings.Split(message, " ")
	event_type, _ := strconv.Atoi(message_list[0]) //need fixed ，create a new type via transaction.

}

