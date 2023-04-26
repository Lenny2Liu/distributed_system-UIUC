package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	servernum  int                 //number of servers
	serverMap  map[string]string   //all other servers mapping  name -> ip:port except itself
	connMap    map[string]net.Conn //ip:port -> connection
	listenMap  map[string]net.Conn //ip:port -> connection
	serverName map[int]string
	myname     string
	myserver   server
	clients    map[string]Client
	nonclients map[string]NonClinetTrans //all non clients transactions in this server, mapper by client addr
)

type Client struct {
	addr          string
	conn          net.Conn
	tx            sync.RWMutex
	trans_list    []*Transaction
	created_accts map[string]*Account
	commit_num    int
	aborted       bool
	accinvolved   map[string]*Account
	servertotalk  []string
	time          int64
}

type server struct {
	name        string
	conn        net.Conn
	acc_list    map[string]*Account //use name as key
	acc_list_tx sync.RWMutex
}

type Account struct {
	balance     int
	rwtx        sync.RWMutex
	access      string //clientaddr
	ReadorWrite int
	created     bool
	commited    bool
	name        string
}

type NonClinetTrans struct {
	id            string //set it equal to the client addr
	tx            sync.Mutex
	trans_list    []*Transaction
	aborted       bool
	created_accts map[string]*Account
}

type Transaction struct {
	account   *Account
	amount    int
	operation string
	// id 		string
}

// parse the config file and return the ip and port of the server
// store the name and connection of other servers in serverMap
func parse_args(name string, config string) (string, string) {
	var ip, port string
	file, err := os.Open(config)
	if err != nil {
		fmt.Println("file does not exist")
		os.Exit(1)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		info := strings.Split(scanner.Text(), " ")
		if info[0] == name {
			ip = info[1]
			port = info[2]
		} else {
			serverMap[info[0]] = info[1] + ":" + info[2]
		}
	}
	return ip, port
}

// handle the message from other servers
func handleServer(conn net.Conn) {

	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error: cannot read from server")
			os.Exit(1)
		}
		fmt.Println("check if server communication is available", msg)
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
		// 2 conditions recieving ABORT:
		// 1. other server is aborting its own client's transaction
		// 2. other server abort transaction of a client in this server
		if msglist[0] == "ABORTING" {
			go abort_trans_from_server(msglist[1])
		} else if msglist[0] == "COMMITING" {
			//recieve commit command from other server, reply COMMIT OK to the server who assigned this command
			go commit_trans_from_server(msglist[1], msglist[2])
		} else if msglist[0] == "DEADLOCK" {
			// receiving DEADLOCK clientaddr1-clientaddr2-clientaddr3-...
			deadlock_detector(msglist[1])
		} else if msglist[0] == "DEPOSITING" || msglist[0] == "WITHDRAWING" {
			// fmt.Println("now I am here you bitch")
			go applychange_from_server(msglist)
		} else if msglist[0] == "COMMITOK" {
			// commit ok handler to record whether received all commitok
			go commitok_handler(msglist[1])
		} else if msglist[0] == "APPLYOK" {
			// apply ok when withdraw's balance is legal
			fmt.Println("receiving apply ok", msg)
			go applyok_handler(msglist[1])
		} else if msglist[0] == "NOTFOUND" {
			// not found when withdraw's balance is illegal
			// msglist: NOTFOUND clientaddr

			go notfound_handler(msglist[1])
		} else if msglist[0] == "BALANCING" {
			// msglist: BALANCING A.abc clientaddr
			go askbalance_from_server(msglist)
		} else if msglist[0] == "BALANCERESULT" {
			// msglist: BALANCERESULT A.abc=100 clientaddr
			go func() {
				addr := strings.Split(msglist[2], "\n")[0]
				fmt.Println("address : ", addr)
				if _, ok := clients[addr]; ok {
					new_client := clients[addr]
					new_client.tx.Lock()
					fmt.Println(new_client.conn.RemoteAddr())
					fmt.Fprintf(new_client.conn, msglist[1]+"\n")
					new_client.tx.Unlock()
				} else {
					fmt.Println("Error: cannot find client 147")
					os.Exit(1)
				}
			}()
		}
	}
}

func dealclients(conn net.Conn, clientaddr string) {
	reader := bufio.NewReader(conn)
	for {
		cmd, _ := reader.ReadString('\n')
		// if err != nil {
		//   fmt.Println("Error: cannot read from client")
		// 	 os.Exit(1)
		// }
		tmp_cmd := strings.Split(cmd, " ")
		fmt.Println("first print", cmd)
		// fmt.Println(tmp_cmd[0])
		if tmp_cmd[0] != "ABORT\n" && tmp_cmd[0] != "COMMIT\n" && tmp_cmd[0] != "BALANCE" && tmp_cmd[0] != "DEPOSIT" && tmp_cmd[0] != "WITHDRAW" {

			return //some command from server2server communication
		}

		client := clients[clientaddr]
		//need to handlae all possible cmd from client
		//DEPOSIT, WITHDRAW, BALANCE(query), COMMIT, ABORT
		fmt.Println("second print", cmd)
		if cmd == "ABORT\n" {
			//need to tell other servers and roll back
			//use lock to make sure only one process can modify the status of this transaction:
			//potention state: now is communicating with other servers for processing this transaction and ABORT cmd is received, must
			//wait for the result of the processing and then roll back
			client.tx.Lock()
			if client.aborted {
				client.tx.Unlock()
				return
			}
			abort_trans_from_client(clientaddr)
			client.tx.Unlock()
			return
		}
		if cmd == "COMMIT\n" {
			// wait for all other servers send back OK then try to commit
			client.tx.Lock()
			commit_transaction(clientaddr)
			client.tx.Unlock()
			return
		}
		// withdraw, deposit contains read/write operation, balance only contains read operation
		command := strings.Split(cmd, " ")
		// fmt.Println(command[0])
		if command[0] == "WITHDRAW" || command[0] == "DEPOSIT" {
			//parse the arguments first
			// fmt.Println("arrive withdraw")
			str := strings.Split(command[2], "\n")[0]
			// transaction := Transaction{}
			amount, _ := strconv.Atoi(str)
			if command[0] == "WITHDRAW" {
				amount = -amount
			}

			// send this command to the server who owns the account
			cmd = command[0] + " " + command[1] + " " + strconv.Itoa(amount)
			//need to tell the server involved in this command to process this transaction

			client.tx.Lock()
			// write changes to the account
			fmt.Println("before applychanges", cmd)
			applychanges(cmd, clientaddr)
			client.tx.Unlock()
			// this function is only involved in communications between servers, so its handler should be in handleServer
		} else if command[0] == "BALANCE" {
			// str_tmp := strings.Split(command[1], ".")
			fmt.Println(cmd)
			client.tx.Lock()
			ask_balance(cmd, clientaddr)
			client.tx.Unlock()
			fmt.Println("release lock")
			// this function is only involved in communications between servers, so its handler should be in handleServer

		} else {
			fmt.Println("Error: invalid command")
			os.Exit(1)
		}
	}
}

func abort_trans_from_server(clientaddr string) {
	clientaddr = strings.Split(clientaddr, "\n")[0]
	if _, ok := clients[clientaddr]; ok {
		// I am recieving abort to my transaction from others
		// need to roll back
		client := clients[clientaddr]
		client.tx.Lock()
		if client.aborted {
			return
		}
		//self rollback
		self_roll_back(clientaddr)
		//send to other servers
		for _, l := range clients[clientaddr].servertotalk {
			msg := "ABORTING " + clientaddr + "\n"
			fmt.Fprintf(connMap[l], msg)
		}
		msg := "ABORTED\n"
		fmt.Fprintf(clients[clientaddr].conn, msg)
		client.tx.Unlock()
	} else {
		// recieving abort to others' transaction from others
		if nonclient, ok := nonclients[clientaddr]; ok {
			nonclient.tx.Lock()
			if nonclient.aborted {
				// already aborted
				nonclient.tx.Unlock()
				return
			} else {
				rollback_other(clientaddr)
				nonclient.tx.Unlock()
			}
		} else {
			fmt.Println("Error: no fucking transaction")
			os.Exit(1)
		}
	}
}

func commit_trans_from_server(clientaddr string, name string) {
	// check whether if the server's account has negative accounts after committing
	fmt.Println("receiving commiting")
	msg := "COMMITOK " + clientaddr + "\n"
	for _, trans := range nonclients[clientaddr].trans_list {
		acct_name := trans.account.name
		// exist negative balance
		acct := myserver.acc_list[acct_name]
		if acct.balance < 0 {
			msg = "ABORTING " + clientaddr + "\n"
		}
		// clear the access clientaddr
		acct.rwtx.RLock()
		if acct.access != clientaddr{
			fmt.Println(acct.access, clientaddr)
			acct.rwtx.RUnlock()
			break
		}
		acct.rwtx.RUnlock()
		acct.rwtx.Lock()
		fmt.Println(acct.access)
		acct.access = ""
		acct.rwtx.Unlock()
		myserver.acc_list[acct_name] = acct
	}
	ser := strings.Split(name, "\n")[0]
	conn := connMap[serverMap[ser]]
	fmt.Fprintf(conn, msg)
	fmt.Println("send back commitOK", conn.RemoteAddr(), msg)
}

func commitok_handler(clientaddr string) {
	fmt.Println("in commitok handler now")
	clientaddr = strings.Split(clientaddr, "\n")[0]
	client := clients[clientaddr]
	client.tx.Lock()
	client.commit_num++
	fmt.Println(client.commit_num, len(client.servertotalk))
	if client.commit_num >= len(client.servertotalk) {
		// tell the client it's commit ok
		fmt.Fprintf(client.conn, "COMMIT OK\n")
	}
	client.tx.Unlock()
	clients[clientaddr] = client
	return
}

func applyok_handler(clientaddr string) {
	clientaddr = strings.Split(clientaddr, "\n")[0]
	client := clients[clientaddr]
	client.tx.Lock()
	fmt.Println(client, clientaddr)
	fmt.Fprintf(client.conn, "OK\n")
	client.tx.Unlock()
}

func notfound_handler(clientaddr string) {
	// tell the client it's not found
	clientaddr = strings.Split(clientaddr, "\n")[0]
	client := clients[clientaddr]
	client.tx.Lock()
	fmt.Fprintf(client.conn, "NOT FOUND, ")
	client.tx.Unlock()
	// Abort
	client.tx.Lock()
	abort_trans_from_client(clientaddr)
	client.tx.Unlock()
}

// e.g. WITHDRAWING A.abc 100 localhost:1234 162736276 servername
func applychange_from_server(cmd []string) {
	var conn net.Conn
	name := strings.Split(cmd[5], "\n")[0]
	conn = connMap[serverMap[name]]
	sev_acc := strings.Split(cmd[1], ".")
	if sev_acc[0] != myname {
		fmt.Println("Error, server name issue 327")
		os.Exit(1)
	}
	clientaddr := cmd[3]
	if _, ok := nonclients[clientaddr]; !ok {
		// set up new nonclient
		nonclient := NonClinetTrans{sev_acc[1], sync.Mutex{}, []*Transaction{}, false, make(map[string]*Account)}
		nonclients[clientaddr] = nonclient
	}
	nonclient := nonclients[clientaddr]
	nonclient.tx.Lock()
	myserver.acc_list_tx.RLock()
	// fmt.Println(cmd[0])
	if _, ok := myserver.acc_list[sev_acc[1]]; !ok && cmd[0] == "DEPOSITING" {
		// set up new account
		fmt.Println("new account")
		myserver.acc_list_tx.RUnlock()
		account := Account{0, sync.RWMutex{}, "", 0, true, false, sev_acc[1]}
		myserver.acc_list_tx.Lock()
		myserver.acc_list[sev_acc[1]] = &account
		myserver.acc_list_tx.Unlock()
		account.rwtx.Lock()
		account.access = clientaddr
		account.rwtx.Unlock()
		nonclient.created_accts[sev_acc[1]] = &account
		nonclient.tx.Unlock()
	} else if _, ok := myserver.acc_list[sev_acc[1]]; !ok && cmd[0] == "WITHDRAWING" {
		myserver.acc_list_tx.RUnlock()
		nonclient.tx.Unlock()
		// not found

		fmt.Fprintf(conn, "NOTFOUND "+clientaddr+"\n")
		// abort_trans_from_server(clientaddr)
	} else {
		myserver.acc_list_tx.RUnlock()
		nonclient.tx.Unlock()
	}
	// isolation implementation

	nonclient.tx.Lock()
	defer nonclient.tx.Unlock()

	// potential deadlock
	for {
		// fmt.Println("waiting for lock")
		// myserver.acc_list[sev_acc[1]].rwtx.Lock()
		if _, ok := myserver.acc_list[sev_acc[1]]; !ok && cmd[0] == "WITHDRAWING" {
			// myserver.acc_list[sev_acc[1]].rwtx.Unlock()
			fmt.Fprintf(conn, "NOTFOUND "+clientaddr+"\n")
			break
		}
		// if _, ok := myserver.acc_list[sev_acc[1]]; !ok && cmd[0] == "BALANCING" {
		// 	// myserver.acc_list[sev_acc[1]].rwtx.Unlock()
		// 	fmt.Fprintf(conn, "NOTFOUND "+clientaddr+"\n")
		// 	break
		// }
		if _, ok := myserver.acc_list[sev_acc[1]]; !ok && cmd[0] == "DEPOSITING" {
			// myserver.acc_list[sev_acc[1]].rwtx.Unlock()
			account := Account{0, sync.RWMutex{}, "", 0, true, false, sev_acc[1]}
			myserver.acc_list_tx.Lock()
			myserver.acc_list[sev_acc[1]] = &account
			myserver.acc_list_tx.Unlock()
			account.rwtx.Lock()
			account.access = clientaddr
			account.rwtx.Unlock()
			nonclient.created_accts[sev_acc[1]] = &account
			// nonclient.tx.Unlock()
			break
		}

		myserver.acc_list[sev_acc[1]].rwtx.Lock()
		// fmt.Println(myserver.acc_list[sev_acc[1]].access, clientaddr)
		if myserver.acc_list[sev_acc[1]].access == "" || myserver.acc_list[sev_acc[1]].access == clientaddr {
			// fmt.Println(myserver.acc_list[sev_acc[1]].access, clientaddr)
			myserver.acc_list[sev_acc[1]].access = clientaddr
			myserver.acc_list[sev_acc[1]].rwtx.Unlock()
			break
		}
		myserver.acc_list[sev_acc[1]].rwtx.Unlock()
	}
	myserver.acc_list_tx.Lock()
	amount, _ := strconv.Atoi(cmd[2])
	myserver.acc_list[sev_acc[1]].balance += amount
	myserver.acc_list_tx.Unlock()
	tmp_nonclient := nonclients[clientaddr]
	acc := myserver.acc_list[sev_acc[1]]
	new_trans := Transaction{acc, amount, cmd[0]}
	tmp_nonclient.trans_list = append(tmp_nonclient.trans_list, &new_trans)
	nonclients[clientaddr] = tmp_nonclient
	// fmt.Println()
	fmt.Fprintf(conn, "APPLYOK "+clientaddr+"\n")
	fmt.Println("APPLYOK ", clientaddr, conn.RemoteAddr(), conn.LocalAddr())

}

func deadlock_detector(formatted string) {

}

// under the client mutex lock
// input cmd: BALANCE/WITHDRAW/BALANCE Server.acc amount
func applychanges(cmd string, clientaddr string) {
	// var new_cmd string
	cmds := strings.Split(cmd, " ")
	sev_acc := strings.Split(cmds[1], ".")
	if sev_acc[0] == myname {
		myserver.acc_list_tx.RLock()
		// check if created
		if _, ok := myserver.acc_list[sev_acc[1]]; !ok && cmds[0] == "DEPOSIT" {
			// set up new account
			myserver.acc_list_tx.RUnlock()
			account := Account{0, sync.RWMutex{}, "", 0, true, false, sev_acc[1]}
			myserver.acc_list_tx.Lock()
			myserver.acc_list[account.name] = &account
			myserver.acc_list_tx.Unlock()
			// mark this account is created by this client
			account.rwtx.Lock()
			account.access = clientaddr
			account.rwtx.Unlock()
			clients[clientaddr].created_accts[account.name] = &account
			if _, ok := clients[clientaddr]; ok {
				clients[clientaddr].created_accts[account.name] = &account
			} else {
				fmt.Println("Error: client not connected to this server")
				os.Exit(1)
			}
		} else if _, ok := myserver.acc_list[sev_acc[1]]; !ok && cmds[0] == "WITHDRAW" {
			// account not exist
			fmt.Println("Error: account not exist")
			myserver.acc_list_tx.RUnlock()
			fmt.Fprintf(clients[clientaddr].conn, "NOT FOUND, ")
			abort_trans_from_client(clientaddr)
			return
		} else {
			myserver.acc_list_tx.RUnlock()
		}

		for {
			myserver.acc_list[sev_acc[1]].rwtx.Lock()
			//fmt.Println("dead in for loop", myserver.acc_list[sev_acc[1]].access)
			if myserver.acc_list[sev_acc[1]].access == "" || myserver.acc_list[sev_acc[1]].access == clientaddr {

				myserver.acc_list[sev_acc[1]].access = clientaddr
				myserver.acc_list[sev_acc[1]].rwtx.Unlock()
				break
			}
			myserver.acc_list[sev_acc[1]].rwtx.Unlock()
		}

		myserver.acc_list_tx.Lock()
		fmt.Println(cmds[2])
		amount, err := strconv.Atoi(cmds[2])
		if err != nil {
			fmt.Println("Error!")
			os.Exit(1)
		}
		myserver.acc_list[sev_acc[1]].balance += amount
		client := clients[clientaddr]
		acc := myserver.acc_list[sev_acc[1]]
		new_trans := Transaction{acc, amount, cmds[0]}
		client.trans_list = append(client.trans_list, &new_trans)
		clients[clientaddr] = client
		fmt.Println(client.trans_list)
		myserver.acc_list_tx.Unlock()
		fmt.Println("OK sent")
		fmt.Fprintf(clients[clientaddr].conn, "OK\n")
		return
	}
	//make it distinctable with command from clients
	//e.g. WITHDRAWING A.abc 100 localhost:1234 162736276
	new_cmd := cmds[0] + "ING " + cmds[1] + " " + cmds[2] + " " + clientaddr + " " + strconv.Itoa(int(clients[clientaddr].time)) + " " + myname + "\n"
	// new_cmd = new_cmd + " " + clientaddr + "\n"
	// send to other servers
	fmt.Println(serverMap)
	if _, ok := serverMap[sev_acc[0]]; !ok {
		fmt.Println("Error: no such server 452")
		os.Exit(1)
	} else {
		// only send this command to the server involved
		found := false
		IP := serverMap[sev_acc[0]]
		client := clients[clientaddr]
		for _, v := range client.servertotalk {
			if v == IP {
				found = true
			}
		}
		if !found {
			client.servertotalk = append(client.servertotalk, IP)
			clients[clientaddr] = client
		}
		fmt.Println(clients[clientaddr].servertotalk)
		// fmt.Println("servermap check", serverMap[sev_acc[0]])
		conn := connMap[serverMap[sev_acc[0]]]
		fmt.Println(conn.RemoteAddr(), conn.LocalAddr())
		_, err := fmt.Fprintf(conn, new_cmd)
		if err != nil {
			fmt.Println("Error: cannot send to server 537")
			os.Exit(1)
		}
		fmt.Println("sent to server")
	}
}

// e.g. BALANCING A.abc clientaddr servername
func askbalance_from_server(cmd []string) {
	// serveracc := strings.Split(cmd[1], "\n")[0]
	sev_acc := strings.Split(cmd[1], ".")
	accname := sev_acc[1]
	servername := strings.Split(cmd[3], "\n")[0]
	conn := connMap[serverMap[servername]]
	client_addr := cmd[2]
	if sev_acc[0] != myname {
		fmt.Println("Error: wrong query sent to this server 428")
		os.Exit(1)
	}
	myserver.acc_list_tx.RLock()
	// if _, ok := nonclients[cmd[2]]; !ok {
	// 	myserver.acc_list_tx.RUnlock()
	// 	fmt.Println("no such nonclient")
	// 	msg := "NOTFOUND " + cmd[2] + "\n"
	// 	fmt.Fprintf(conn, msg)
	// 	return
	// }
	fmt.Println("accname", accname, conn.RemoteAddr(), myserver.acc_list[accname])
	// lock until
	for {
		if _, ok := myserver.acc_list[accname]; !ok {
			fmt.Println("no such account")
			myserver.acc_list_tx.RUnlock()
			fmt.Fprintf(conn, "NOTFOUND "+cmd[2]+"\n")
			return
		} else {
			myserver.acc_list[sev_acc[1]].rwtx.Lock()
			// fmt.Println(myserver.acc_list[sev_acc[1]].access, client_addr)
			if myserver.acc_list[sev_acc[1]].access == "" || myserver.acc_list[sev_acc[1]].access == client_addr {
				fmt.Println(myserver.acc_list[sev_acc[1]].access, client_addr)
				myserver.acc_list[sev_acc[1]].access = client_addr
				myserver.acc_list[sev_acc[1]].rwtx.Unlock()
				break
			}
			myserver.acc_list[sev_acc[1]].rwtx.Unlock()
		}
	}
	// BALANCERESULT A.abc=100 clientaddr
	if _, ok := clients[client_addr]; ok {
		client := clients[client_addr]
		client.tx.Lock()
		// client := clients[clientaddr]
		myserver.acc_list[sev_acc[1]].rwtx.RLock()
		accnt := myserver.acc_list[accname]
		myserver.acc_list[sev_acc[1]].rwtx.RUnlock()
		client.trans_list = append(client.trans_list, &Transaction{accnt, 0, ""})
		clients[client_addr] = client
		client.tx.Unlock()
	} else {
		nonclient := nonclients[client_addr]
		nonclient.tx.Lock()
		myserver.acc_list[sev_acc[1]].rwtx.RLock()
		accnt := myserver.acc_list[accname]
		myserver.acc_list[sev_acc[1]].rwtx.RUnlock()
		nonclient.trans_list = append(nonclient.trans_list, &Transaction{accnt, 0, ""})
		nonclient.tx.Unlock()
		nonclients[client_addr] = nonclient
	}
	fmt.Fprintf(conn, "BALANCERESULT "+cmd[1]+"="+strconv.Itoa(myserver.acc_list[accname].balance)+" "+cmd[2]+"\n")
	myserver.acc_list_tx.RUnlock()
	fmt.Println("sent result to server", conn.RemoteAddr())

}

func ask_balance(cmd string, clientaddr string) {
	cmds := strings.Split(cmd, " ")
	sev_acc := strings.Split(cmds[1], ".")
	serveracc := strings.Split(cmds[1], "\n")[0]
	accname := strings.Split(sev_acc[1], "\n")[0]
	fmt.Println(accname)
	if sev_acc[0] == myname {
		for {
			//myserver.acc_list_tx.RLock()
			// fmt.Println(myserver.acc_list[sev_acc[1]], sev_acc[1])
			if _, ok := myserver.acc_list[accname]; !ok {
				fmt.Println("NOT FOUND")
				//myserver.acc_list_tx.RUnlock()
				fmt.Fprintf(clients[clientaddr].conn, "NOT FOUND, ")
				abort_trans_from_client(clientaddr)
				return
			} else {
			// use read lock here
				myserver.acc_list[accname].rwtx.Lock()
				fmt.Println(myserver.acc_list[accname].access, clientaddr)
				if myserver.acc_list[accname].access == "" || myserver.acc_list[accname].access == clientaddr {
					fmt.Println(myserver.acc_list[accname].access, clientaddr)
					myserver.acc_list[accname].access = clientaddr
					myserver.acc_list[accname].rwtx.Unlock()
					break
				}
				myserver.acc_list[accname].rwtx.Unlock()
			}
		}
		client := clients[clientaddr]
		accnt := myserver.acc_list[accname]
		client.trans_list = append(client.trans_list, &Transaction{accnt, 0, ""})
		clients[clientaddr] = client
		fmt.Println(myserver.acc_list[accname].balance)
		fmt.Fprintf(clients[clientaddr].conn, serveracc+"="+strconv.Itoa(myserver.acc_list[accname].balance)+"\n")
		//myserver.acc_list_tx.RUnlock()
		return
	}
	//make it distinctable with command from clients
	new_cmd := "BALANCING" + " " + serveracc + " " + clientaddr + " " + myname + "\n"
	// send to other servers
	if _, ok := serverMap[sev_acc[0]]; !ok {
		fmt.Println("Error: no such server 487")
		os.Exit(1)
	} else {
		// only send this command to the server involved
		// IP := serverMap[sev_acc[0]]
		// client := clients[clientaddr]
		// client.servertotalk = append(client.servertotalk, IP)
		// clients[clientaddr] = client
		// client := clients[clientaddr]
		// accnt := myserver.acc_list[accname]
		// client.trans_list = append(client.trans_list, &Transaction{accnt, 0, ""})
		// clients[clientaddr] = client
		found := false
		IP := serverMap[sev_acc[0]]
		client := clients[clientaddr]
		for _, v := range client.servertotalk {
			if v == IP {
				found = true
			}
		}
		if !found {
			client.servertotalk = append(client.servertotalk, IP)
			clients[clientaddr] = client
		}
		conn := connMap[serverMap[sev_acc[0]]]
		fmt.Fprintf(conn, new_cmd)
	}
}

// rollback other

func rollback_other(clientaddr string) {
	myserver.acc_list_tx.RLock()
	Acctlist := myserver.acc_list
	myserver.acc_list_tx.RUnlock()
	fmt.Println(len(nonclients[clientaddr].trans_list))
	fmt.Println(nonclients[clientaddr].trans_list[0].operation, nonclients[clientaddr].trans_list[0].amount)
	for _, trans := range nonclients[clientaddr].trans_list {
		// ACCOUNT is created by client
		Acc := trans.account
		Aname := Acc.name
		Acc.rwtx.RLock()
		if _, ok := Acctlist[Aname]; ok {
			Acc.rwtx.RUnlock()
			if _, ok := nonclients[clientaddr].created_accts[Aname]; ok {
				myserver.acc_list_tx.Lock()
				delete(Acctlist, Aname)
				myserver.acc_list_tx.Unlock()
			} else {
				// ACCOUNT is not created by client
				if trans.operation == "DEPOSITING" {
					Acctlist[Aname].rwtx.Lock()
					Acctlist[Aname].balance -= trans.amount
					Acctlist[Aname].rwtx.Unlock()
				} else if trans.operation == "WITHDRAWING" {
					Acctlist[Aname].rwtx.Lock()
					Acctlist[Aname].balance -= trans.amount
					Acctlist[Aname].rwtx.Unlock()
				}
				Acctlist[Aname].rwtx.Lock()
				Acctlist[Aname].access = ""
				Acctlist[Aname].rwtx.Unlock()
			}
		} else {
			Acc.rwtx.RUnlock()
			// already be deleted by this server
			continue
		}
	}
}

// self roll back the client transaction by the clientaddr
func self_roll_back(clientaddr string) {
	myserver.acc_list_tx.RLock()
	Acctlist := myserver.acc_list
	myserver.acc_list_tx.RUnlock()
	fmt.Println(len(clients[clientaddr].trans_list))
	if len(clients[clientaddr].trans_list) == 0 {
		return
	}
	for _, trans := range clients[clientaddr].trans_list {
		// ACCOUNT is created by client
		Acc := trans.account
		Aname := Acc.name
		Acc.rwtx.RLock()
		if _, ok := Acctlist[Aname]; ok {
			Acc.rwtx.RUnlock()
			if _, ok := clients[clientaddr].created_accts[Aname]; ok {
				myserver.acc_list_tx.Lock()
				delete(Acctlist, Aname)
				delete(clients[clientaddr].created_accts, Aname)
				myserver.acc_list_tx.Unlock()
			} else {
				// ACCOUNT is not created by client
				if trans.operation == "DEPOSIT" {
					Acctlist[Aname].rwtx.Lock()
					Acctlist[Aname].balance -= trans.amount
					Acctlist[Aname].rwtx.Unlock()
				} else if trans.operation == "WITHDRAW" {
					Acctlist[Aname].rwtx.Lock()
					Acctlist[Aname].balance -= trans.amount
					Acctlist[Aname].rwtx.Unlock()
				}
				// clear the account's access client
				Acctlist[Aname].rwtx.Lock()
				Acctlist[Aname].access = ""
				Acctlist[Aname].rwtx.Unlock()
			}
		} else {
			Acc.rwtx.RUnlock()
			continue
		}
	}
}

// this function is inside client.tx.Lock()/Unlock()
func abort_trans_from_client(clientaddr string) {
	self_roll_back(clientaddr)
	//send to other servers
	fmt.Println(clients[clientaddr].servertotalk)
	if len(clients[clientaddr].servertotalk) == 0 {
		fmt.Println(clients[clientaddr].conn.LocalAddr(), clients[clientaddr].conn.RemoteAddr())
		msg := "ABORTED\n"
		fmt.Fprintf(clients[clientaddr].conn, msg)
	}
	for _, l := range clients[clientaddr].servertotalk {
		msg := "ABORTING " + clientaddr + "\n"
		fmt.Fprintf(connMap[l], msg)
		fmt.Fprintf(clients[clientaddr].conn, "ABORTED\n")
	}
	// client.tx.Unlock()
}

// Commit msg: COMMITED
func commit_transaction(clientaddr string) {
	// sending COMMITING msg to servers
	client := clients[clientaddr]
	// client.tx.Lock()
	// fmt.Println(client.servertotalk)
	if len(client.servertotalk) != 0 {
		// msg := "COMMIT OK\n"
		// fmt.Fprintf(client.conn, msg)
		for _, l := range clients[clientaddr].servertotalk {
			fmt.Println("sending COMMITING msg to server", l)
			msg := "COMMITING " + clientaddr + " " + myname + "\n"
			fmt.Fprintf(connMap[l], msg)
		}
	}

	// unlock myserver's account lock from clientaddr
	//fmt.Println(len(clients[clientaddr].trans_list))
	if len(clients[clientaddr].trans_list) != 0 {
		if len(client.servertotalk) != 0{
			for _, trans := range clients[clientaddr].trans_list {
			// fmt.Println(*trans)
				Aname := trans.account.name
			// fmt.Println("what the fuck man", Aname)
				if _, ok := myserver.acc_list[Aname]; ok {
					myserver.acc_list[Aname].rwtx.Lock()
					myserver.acc_list[Aname].access = ""
					if myserver.acc_list[Aname].balance < 0 {
						fmt.Println(myserver.acc_list[Aname].balance)
						myserver.acc_list[Aname].rwtx.Unlock()
						abort_trans_from_client(clientaddr)
						return
					}
					myserver.acc_list[Aname].rwtx.Unlock()
				}
			}
		}
		if len(client.servertotalk) == 0 {
			for _, trans := range clients[clientaddr].trans_list {
				Aname := trans.account.name
				fmt.Println("what the fuck man", Aname)
				if _, ok := myserver.acc_list[Aname]; ok {
					myserver.acc_list[Aname].rwtx.Lock()
					myserver.acc_list[Aname].access = ""
					if myserver.acc_list[Aname].balance < 0 {
						fmt.Println(myserver.acc_list[Aname].balance)
						myserver.acc_list[Aname].rwtx.Unlock()
						abort_trans_from_client(clientaddr)
						return
						}
					myserver.acc_list[Aname].rwtx.Unlock()
					}
			}
			fmt.Fprintf(clients[clientaddr].conn, "COMMIT OK\n")
			
			return
		}
	}
	if len(client.servertotalk) == 0 {
		msg := "COMMIT OK\n"
		// if _, ok := myserver.acc_list[]
		fmt.Fprintf(client.conn, msg)
	}

	client.commit_num++
	clients[clientaddr] = client
	// client.tx.Unlock()
}

func getlocaltime() int64 {
	return time.Now().UnixNano()
}

func main() {
	var i int
	argv := os.Args
	if len(argv) != 3 {
		fmt.Println("Usage: ./server <server_name> <config_file>")
		os.Exit(1)
	}
	serverMap = make(map[string]string)
	connMap = make(map[string]net.Conn)
	clients = make(map[string]Client)
	nonclients = make(map[string]NonClinetTrans)
	listenMap = make(map[string]net.Conn)
	servername := argv[1]
	myname = servername
	servernum = 5
	serverIP, serverPort := parse_args(servername, argv[2])
	fmt.Println(serverIP, serverPort)
	myserver = server{name: myname, conn: nil, acc_list: make(map[string]*Account)}
	if serverIP == "" || serverPort == "" {
		fmt.Println("No such server 678")
		os.Exit(1)
	}

	//k is the name of server we are going to connect to while v is its ip:port
	listener, err := net.Listen("tcp", serverIP+":"+serverPort)
	if err != nil {
		fmt.Println("Error: cannot listen to port", serverPort, err)
		os.Exit(1)
	}
	time.Sleep(10 * time.Second)
	// fmt.Println("serverMap", serverMap)
	for k, v := range serverMap {
		// fmt.Println("k", k, "v", v)
		conn, err := net.Dial("tcp", v)
		if err != nil {
			fmt.Println("Error: cannot connect to server", k, err)
			os.Exit(1)
		}
		connMap[v] = conn
		defer conn.Close()

	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error: cannot accept connection")
			os.Exit(1)
		}
		addr := conn.RemoteAddr().String()
		fmt.Println("New connection from", addr, i)
		if _, ok := connMap[addr]; ok {
			// listen from other servers, omit here
			fmt.Println("msg from server: ", addr)
			continue
		}

		if i < 4 {
			listenMap[addr] = conn
			i++
			fmt.Println(listenMap)
			go handleServer(conn)
			continue
		}
		fmt.Println("address of server/client", addr)
		if _, ok := listenMap[addr]; ok {
			fmt.Println("msg from server: ", addr)
			continue
		}
		// fmt.Println("New connection from", addr)
		clients[addr] = Client{addr, conn, sync.RWMutex{}, []*Transaction{}, make(map[string]*Account), 0, false, make(map[string]*Account), []string{}, getlocaltime()}
		// fmt.Println(clients)
		go dealclients(conn, addr)
	}
}
