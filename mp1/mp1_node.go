package main

import(
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
	"bufio"
	"flag"
	"sort"
	"sync"
)

type node_info struct{
	if_active bool
	conn net.Conn
}

type node struct{
	node_name string
	node_balance int32
	info node_info
}

type transcation struct{
	delivarable bool
	transaction_type int32
	transaction_num int32
	transaction_amount int32
	transaction_src node
	transaction_dest node
	agreed_order int32
	proposed_order float64
	index int
}

var(
	pq priority_queue
	transaction_list []transcation
	node_list []node
	tx sync.Mutex
)










func main(){

}