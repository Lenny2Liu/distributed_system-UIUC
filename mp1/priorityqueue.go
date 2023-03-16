package main

//implementation of priority queue is from https://pkg.go.dev/container/heap

import (
	"container/heap"
	//"fmt"
	//"sync"
)

type transcation struct {
	delivarable        bool
	transaction_type   bool // == transfer
	transaction_amount int
	transaction_src    account
	transaction_dest   account
	proposed_order     float64
	agreed_order       float64
	mulnode_num        int // record how many nodes it should multicast
	uninode_num        int
	index              int
	msg_id			   string
	//assign_time 	   float64
	//trans_lock		   sync.Mutex
}

type account struct {
	acct_name    string
	acct_bala    int
	acct_present bool
}

type priority_queue []*transcation

func Create() priority_queue {
	pq := make(priority_queue, 0)
	heap.Init(&pq)
	return pq
}

func contains(x transcation, pq priority_queue) bool {
	for _, v := range pq {
		if v.index == x.index {
			return true
		}
	}
	return false
}

func (pq priority_queue) Len() int {
	return len(pq)
}

func (pq *priority_queue) Push(x interface{}) {
	transac := x.(*transcation)
	if contains(*transac, *pq) {
		return
	}
	transac.index = len(*pq)
	*pq = append(*pq, transac)
}

func (pq *priority_queue) Pop() interface{} {
	if len(*pq) == 0 {
		return transcation{}
	}
	old := *pq
	n := len(old)
	transac := old[n-1]
	*pq = old[0 : n-1]
	return transac
}

func (pq *priority_queue) Top() interface{} {
	if len(*pq) == 0 {
		return transcation{}
	}
	new := *pq
	return new[0]
}

func (pq priority_queue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq priority_queue) Less(i, j int) bool {
	return pq[i].proposed_order < pq[j].proposed_order
}

func (pq *priority_queue) update(x *transcation, proposed_order float64) {
	if !contains(*x, *pq) {
		return
	}
	if proposed_order > x.proposed_order {
		x.proposed_order = proposed_order
	}
	heap.Fix(pq, x.index)
}

func (pq *priority_queue) remove(x transcation) {
	if !contains(x, *pq) {
		return
	}
	heap.Remove(pq, x.index)
}
