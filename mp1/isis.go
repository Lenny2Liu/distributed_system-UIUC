package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
)

type message struct {
	ID        int
	Timestamp time.Time
	Content   string
}

type process struct {
	ID    int
	Known map[int]time.Time
	mutex sync.Mutex
}

func (p *process) send(msg message, conn net.Conn) {
	msg.Timestamp = time.Now()
	msgStr := fmt.Sprintf("%d:%d:%s\n", msg.ID, msg.Timestamp.UnixNano(), msg.Content)
	conn.Write([]byte(msgStr))
}

func (p *process) receive(conn net.Conn) {
	reader := bufio.NewReader(conn)

	for {
		msgStr, err := reader.ReadString('\n')
		if err != nil {
			break
		}

		// parse message string
		msg := message{}
		msgFields := bufio.NewScanner(reader)
		msgFields.Split(bufio.ScanWords)

		msg.ID, _ = strconv.Atoi(msgFields.Text())
		msgTimestamp, _ := strconv.ParseInt(msgFields.Text(), 10, 64)
		msg.Timestamp = time.Unix(0, msgTimestamp)
		msg.Content = msgFields.Text()

		p.mutex.Lock()
		if _, ok := p.Known[msg.ID]; !ok {
			p.Known[msg.ID] = msg.Timestamp
			fmt.Printf("Process %d received message from process %d with content '%s'\n", p.ID, msg.ID, msg.Content)

			// send the message to all other processes
			for i := 0; i < numProcesses; i++ {
				if i != p.ID {
					conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", basePort+i))
					if err == nil {
						p.send(msg, conn)
						conn.Close()
					}
				}
			}
		}
		p.mutex.Unlock()
	}
}
