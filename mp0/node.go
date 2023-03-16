package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func main() {
	arguments := os.Args
	scan := bufio.NewScanner(os.Stdin)
	if len(arguments) != 4 {
		fmt.Println("Please provide 4 arguments")
		return
	}
	PORT := arguments[2] + ":" + arguments[3]
	node := arguments[1]
	l, err := net.Dial("tcp", PORT)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer l.Close()
	l.Write([]byte(node + "\n"))
	for scan.Scan() {
		l.Write([]byte(scan.Text() + "\n"))
	}

}
