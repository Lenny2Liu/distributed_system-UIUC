package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sasha-s/go-deadlock"
)

type node_info struct {
	if_active bool
	conn      net.Conn
}

type node struct {
	node_name       string
	connected_nodes []string
	info            node_info
	msg_num         int
}

type msg struct {
	msg_id   string
	msg_type string
	trs      transcation
}

var (
	pq                  = Create()
	transaction_list    []transcation
	node_list           []node
	acct_list           []account
	message_sent        = make(map[string]bool)
	sent                = make(map[string]bool)
	msgtransac          = make(map[string]*transcation)
	account_map         = make(map[string]account)
	safe_tx             deadlock.Mutex
	safe_tx_communicate deadlock.Mutex
	some_tx             deadlock.Mutex
	wg                  sync.WaitGroup
	time_process        *os.File
	failure             []bool
	node_n              int
	fl_lock             deadlock.Mutex
	//node_ind            int
)

func real_time() float64 {
	return float64(time.Now().UnixMicro()) / float64(1000000)
}

func Multicast(event string) {
	for i := range node_list {
		//fmt.Println("das ist event:", event)
		if failure[i] == false {
			_, err := node_list[i].info.conn.Write([]byte(event))
			if err != nil {
				defer node_list[i].info.conn.Close()
				failure[i] = true
				//fmt.Println(event)
				node_n = node_n - 1
				go fail_handler(i)
				continue
			}
		}
	}
}

func unicast(conn net.Conn, event string) {
	conn.Write([]byte(event))
}

func communicate(node_name string, node_host string, node_port int, node_index int) {
	//.Println("node_port: ", node_port)
	conn, err := net.Dial("tcp", node_host+":"+strconv.Itoa(node_port))
	for err != nil {
		time.Sleep(time.Second)
		//fmt.Println("Error: ", err)
		conn, err = net.Dial("tcp", node_host+":"+strconv.Itoa(node_port))
	}
	//node_list[node_index].tx_communicate.Lock()
	//fmt.Println("node_index: ", node_index)
	node_list[node_index].connected_nodes = append(node_list[node_index].connected_nodes, node_name)
	//node_list[node_index].tx_communicate.Unlock()
	for i := range node_list {
		if node_list[i].node_name == node_name {
			node_list[i].info.if_active = true
			node_list[i].info.conn = conn
		}
	}
}

func read_config(filename string) (node_num int, node_name []string, node_host []string, node_port []int) {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Error: ", err)
		os.Exit(1)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Scan()
	node_num, _ = strconv.Atoi(scanner.Text())
	node_n = node_num
	// adding the failure matrix 1:failure 0:not failure
	for j := 0; j < node_n; j++ {
		failure = append(failure, false)
	}
	for scanner.Scan() {
		line_components := strings.Split(scanner.Text(), " ")
		node_name = append(node_name, line_components[0])
		node_host = append(node_host, line_components[1])
		port, _ := strconv.Atoi(line_components[2])
		node_port = append(node_port, port)
	}
	//fmt.Printf("node_num: %d", node_num)
	return node_num, node_name, node_host, node_port
}

func main() {
	var i int
	var speaker int
	var node_index int
	runtime.SetBlockProfileRate(1)     //block
	runtime.SetMutexProfileFraction(1) //mutex
	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()
	args := os.Args
	identifier := args[1]
	//fmt.Println("identifier: ", identifier)
	node_num, node_name, node_host, node_port := read_config(args[2])
	for i < node_num {
		if node_name[i] == identifier {
			speaker = node_port[i]
			node_index = i
		}
		node := node{node_name: node_name[i], connected_nodes: []string{}, info: node_info{if_active: false, conn: nil}, msg_num: 0}
		node_list = append(node_list, node)
		i++
	}
	//node_ind = node_index
	time_path := os.Args[1] + "_Processing.txt"
	tmp, err := os.Create(time_path)
	if err != nil {
		fmt.Println("Error: ", err)
		os.Exit(1)
	}
	time_process = tmp
	os.Truncate(time_path, 0)
	defer time_process.Close()
	//fmt.Println("this is length of node_list", len(node_list))
	if speaker == 0 {
		fmt.Println("Error: ", "Speaker is not found")
		os.Exit(1)
	}
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(speaker))
	if err != nil {
		fmt.Println("Error: ", err)
		os.Exit(1)
	}
	defer listener.Close()
	i = 0
	for i < node_num {
		if node_name[i] == identifier {
			speaker = node_port[i]
			node_index = i
		}
		communicate(node_name[i], node_host[i], node_port[i], node_index)
		i++
		//rintln(i)
	}

	for i = 0; i < node_num; i++ {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error: ", err)
			os.Exit(1)
		}

		go connection_handler(conn, node_index)
	}
	wg.Add(1)
	go send_msg(node_index, node_num)
	wg.Wait()
}

func connection_handler(conn net.Conn, node_index int) {
	defer conn.Close()
	reader := bufio.NewScanner(conn)
	for reader.Scan() {
		message := reader.Text()
		safe_tx.Lock()
		if message == "" || message == "\n" || message_sent[message] {
			safe_tx.Unlock()
			continue
		} else {
			//safe_tx.Unlock()
			//fmt.Printf("message: %s \n", message)
			//safe_tx.Lock()

			message_sent[message] = true
			safe_tx.Unlock()
			message_list := strings.Split(message, " ")
			event_type := message_list[0]

			switch event_type {
			case "initiate":
				initiate_handler(message_list, node_index)
			case "proposed":
				proposed_handler(message_list, node_index)
			case "final":
				//fmt.Println("important", message_sent[message])
				final_handler(message_list, node_index)
			default:
				fmt.Printf("Error: ", "Invalid event type \n")
			}
		}
	}
}

/* types: bool
 * 		0: deposit 1: transfer
 */
func handle_balance(types bool, acct1 string, acct2 string, money int) {
	// transfer account1 -> account2
	if types {
		person1 := findAcctByName(acct1)
		if person1 == nil {
			return
		}
		if (person1.acct_bala - money) < 0 {
			//print the balance of each accts
			fmt.Printf("Balance ")
			for i := 0; i < len(acct_list); i++ {
				fmt.Printf(acct_list[i].acct_name + ":" + strconv.Itoa(acct_list[i].acct_bala) + " ")
			}
			fmt.Printf("\n")
			return
		} else {
			person1.acct_bala = person1.acct_bala - money
		}
		person2 := findAcctByName(acct2)
		if person2 == nil {
			person := account{acct2, money, true}
			acct_list = append(acct_list, person)
			sort.Slice(acct_list, func(i, j int) bool {
				return acct_list[i].acct_name < acct_list[j].acct_name
			})
		} else {
			person2.acct_bala = person2.acct_bala + money
		}
	} else {
		// deposit  account1 =  account2 from=to
		person1 := findAcctByName(acct1)
		if person1 == nil {
			person := account{acct1, money, true}
			acct_list = append(acct_list, person)
			//Sort the slice by name field in alphabetical order
			sort.Slice(acct_list, func(i, j int) bool {
				return acct_list[i].acct_name < acct_list[j].acct_name
			})
		} else {
			person1.acct_bala = money
		}
	}
	//print the balance of each accts
	fmt.Printf("Balance ")
	for i := 0; i < len(acct_list); i++ {
		if acct_list[i].acct_bala < 0 {
			fmt.Printf("Error: ", "Negative balance \n")
			os.Exit(1)
		}
		fmt.Printf(acct_list[i].acct_name + ":" + strconv.Itoa(acct_list[i].acct_bala) + " ")
	}
	fmt.Printf("\n")
}

func findAcctByName(targetName string) *account {
	for i := 0; i < len(acct_list); i++ {
		if acct_list[i].acct_name == targetName {
			return &acct_list[i]
		}
	}
	return nil
}

func send_msg(i int, node_num int) {
	var amount int
	var src account
	var dst account
	for {
		safe_tx.Lock()
		//fmt.Println(len(node_list[i].connected_nodes))
		if len(node_list[i].connected_nodes) == node_num {
			safe_tx.Unlock()
			break
		}
		safe_tx.Unlock()
	}

	scan := bufio.NewScanner(os.Stdin)
	for scan.Scan() {
		message := scan.Text()
		msg_list := strings.Split(message, " ")
		safe_tx.Lock()
		if msg_list[0] == "TRANSFER" {
			amount, _ = strconv.Atoi(msg_list[4])
			src, dst = check_account(msg_list[1], msg_list[3])
		} else {
			amount, _ = strconv.Atoi(msg_list[2])
			src, dst = check_account(msg_list[1], msg_list[1])
		}
		node_list[i].msg_num++
		msg_id := strconv.Itoa(i) + "-" + strconv.Itoa(node_list[i].msg_num)
		trans := transcation{false, msg_list[0] == "TRANSFER", amount, src, dst, 0, 0, node_n - 1, 0, len(transaction_list), msg_id}
		// msgtransac["1-1"] = &trans
		transaction_list = append(transaction_list, trans)

		trans.proposed_order = float64(i)*0.1 + float64(node_list[i].msg_num)
		time_process.WriteString(strconv.FormatFloat(real_time(), 'f', 6, 64) + " " + msg_id + " initialize\n")
		heap.Push(&pq, &trans)
		pq.update(&trans, trans.proposed_order)
		// msg_id := strconv.Itoa(i) + "_" + strconv.Itoa(node_list[i].msg_num)
		msgtransac[msg_id] = &trans
		//fmt.Println(msgtransac[msg_id])
		safe_tx.Unlock()
		message = "initiate " + message + " " + strconv.Itoa(trans.index) + " " + msg_id + " " + node_list[i].node_name + "\n"
		//fmt.Printf("message: %s", message)
		Multicast(message)
	}
}

func initiate_handler(message_list []string, node_index int) {
	var sender node
	var amount int
	var sender_index int
	var src account
	var dst account
	sender_name := message_list[len(message_list)-1]
	// if _, ok := msgtransac[]; ok {
	// 	fmt.Println("Key 'b' is present in the map")
	// } else {
	// 	fmt.Println("Key 'b' is not present in the map")
	// }
	msg_id := message_list[len(message_list)-2]
	//fmt.Println(msg_id)
	for i := 0; i < len(node_list); i++ {
		if node_list[i].node_name == sender_name {
			sender = node_list[i]
			sender_index = i
			break
		}
	}
	if sender_index == node_index {
		return
	}
	safe_tx.Lock()
	msg_list := message_list
	//fmt.Println(message_list)
	//fmt.Println(msg_list[1])
	if msg_list[1] == "TRANSFER" {
		amount, _ = strconv.Atoi(msg_list[5])
		src, dst = check_account(msg_list[2], msg_list[4])
	} else {
		amount, _ = strconv.Atoi(msg_list[3])
		src, dst = check_account(msg_list[2], msg_list[2])
	}
	//fmt.Println(len(transaction_list))
	//fmt.Println(msg_list[1])
	trans := transcation{false, msg_list[1] == "TRANSFER", amount, src, dst, 0, 0, node_n - 1, 0, len(transaction_list), msg_id}
	transaction_list = append(transaction_list, trans)
	node_list[node_index].msg_num++
	msgtransac[msg_id] = &trans
	trans.proposed_order = float64(node_index)*0.1 + float64(node_list[node_index].msg_num)
	heap.Push(&pq, &trans)
	pq.update(&trans, trans.proposed_order)
	proposed_order := strconv.FormatFloat(trans.proposed_order, 'f', 1, 64)
	proposed := "proposed " + " " + proposed_order + " " + msg_id + "\n"
	//fmt.Println(sender.node_name, msg_id, "\n")
	safe_tx.Unlock()
	safe_tx_communicate.Lock()

	// unicast(sender.info.conn, proposed)
	_, err := sender.info.conn.Write([]byte(proposed))
	if err != nil {
		sender.info.if_active = false
		safe_tx_communicate.Unlock()
		failure[sender_index] = true
		node_n = node_n - 1
		go fail_handler(sender_index)
	}
	safe_tx_communicate.Unlock()
}

/*
 * message_list:
 * 		proposed proposed_order [1-1]
 */
func proposed_handler(message_list []string, node_index int) {
	// sender = original sender who multicast
	var prorder float64
	//fmt.Println(message_list)
	//fmt.Println("\n")

	trans_index := message_list[3]
	prorder, _ = strconv.ParseFloat(message_list[2], 64)
	safe_tx.Lock()
	trans := msgtransac[trans_index]
	// find the pq's priority queue
	// update transaction's proposed_order
	if prorder > trans.proposed_order {
		trans.proposed_order = prorder
		//fmt.Println(trans.proposed_order)
	}
	trans.delivarable = false
	trans.uninode_num++
	trans.mulnode_num = node_n - 1
	safe_tx.Unlock()
	if trans.uninode_num >= trans.mulnode_num {
		// multicast message: final agreed_order trans_index sender_index
		safe_tx_communicate.Lock()
		heap.Fix(&pq, trans.index)
		message := "final " + strconv.FormatFloat(trans.proposed_order, 'f', 1, 64) + " " + trans_index + "\n"
		//fmt.Println("num_final\n")
		safe_tx_communicate.Unlock()
		//fmt.Println(message)
		Multicast(message)
	}
}

/*
 * final handler
 * 		received message: final agreed_order trans_index
 */
func final_handler(message_list []string, node_index int) {

	safe_tx.Lock()

	agree_order, _ := strconv.ParseFloat(message_list[1], 64)
	trans_index := message_list[2]

	trans := msgtransac[trans_index]
	// check := 0
	// for i := 0; i < len(pq); i++ {
	// 	if pq[i] == trans {
	// 		check = 1
	// 	}
	// }
	// // not present
	// if check == 0 {
	// 	heap.Push(&pq, trans)
	// }
	trans.delivarable = true
	trans.proposed_order = agree_order
	//fmt.Println(trans.index)
	heap.Fix(&pq, trans.index)

	safe_tx.Unlock()
	safe_tx.Lock()

	for pq.Len() != 0 && pq[0].delivarable == true {
		trans := heap.Pop(&pq).(*transcation)
		//fmt.Println(trans.transaction_type, trans.transaction_src.acct_name, trans.transaction_dest.acct_name, trans.transaction_amount)
		handle_balance(trans.transaction_type, trans.transaction_src.acct_name, trans.transaction_dest.acct_name, trans.transaction_amount)
		time_process.WriteString(strconv.FormatFloat(real_time(), 'f', 6, 64) + " " + trans.msg_id + " final\n")
		if pq.Len() == 0 {
			break
		}
	}
	safe_tx.Unlock()
}

func fail_handler(fail_index int) {
	fl_lock.Lock()
	failure[fail_index] = true
	fl_lock.Unlock()
	//remove the transaction in pq, which final priority will never be determined.
	// safe_tx.Lock()
	// //fmt.Println(pq[0])
	// for i := 0; i < len(pq); i++ {
	// 	trans := pq[i]
	// 	id_msg_list := strings.Split(trans.msg_id, "-")
	// 	msg_from_node, _ := strconv.Atoi(id_msg_list[0])
	// 	//fmt.Println(trans)
	// 	if trans.delivarable && msg_from_node == fail_index {
	// 		//fmt.Println("multicast time!!!!!!!!!")
	// 		Multicast("final " + strconv.FormatFloat(trans.proposed_order, 'f', 1, 64) + " " + trans.msg_id + "\n")
	// 	}
	// }
	//safe_tx.Unlock()
	//fmt.Println(pq[0])
	clean_q(fail_index)
}

func clean_q(fail_index int) {
	time.Sleep(5 * time.Second)
	safe_tx.Lock()
	for i := 0; i < len(pq); i++ {
		trans := pq[i]
		if trans.delivarable == false {
			heap.Remove(&pq, i)
			i--
		}
	}
	safe_tx.Unlock()
}

func check_account(account_1 string, account_2 string) (account, account) {
	var new_acc_1 account
	var new_acc_2 account
	some_tx.Lock()
	_, ok := account_map[account_1]
	if !ok {
		new_acc_1 = account{account_1, 0, true}
		//acct_list = append(acct_list, new_acc_1)
	} else {
		new_acc_1 = account_map[account_1]
	}
	_, ok = account_map[account_2]
	if !ok {
		new_acc_2 = account{account_2, 0, true}
		//acct_list = append(acct_list, new_acc_2)
	} else {
		new_acc_2 = account_map[account_2]
	}
	some_tx.Unlock()
	return new_acc_1, new_acc_2
}

