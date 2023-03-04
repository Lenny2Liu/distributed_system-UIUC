package main

//implementation of priority queue is from https://pkg.go.dev/container/heap

import (
	"container/heap"
	"fmt"
	"io"
	"net"
)


type priority_queue []transcation

func Create() priority_queue{
	pq = make(priority_queue, 0)
	heap.Init(pq)
	return pq
}

func contains(x transcation, pq priority_queue) bool{
	for _, v := range pq{
		if v == x{
			return true
		}
	}
	return false
}

func (pq priority_queue) Len() int { 
	return len(pq) 
}

func (pq priority_queue) Push(x interface{}) { 
	transac := x.(transcation)
	if contains(transac, pq){
		return
	}
	transac.index = len(pq)
	pq = append(pq, transac) 
}

func (pq priority_queue) Pop()(x interface{}) {
	if len(pq) == 0{
		return transcation{}
	}
	transac := x.(transcation)
	transac = pq[len(pq)-1]
	pq = pq[:len(pq)-1]
	return transac
}

func (pq priority_queue) Top()(transcation) {
	if len(pq) == 0{
		return transcation{}
	}
	return pq[0]
}

func (pq priority_queue) Swap(i, j int) { 
	pq[i], pq[j] = pq[j], pq[i] 
	pq[i].index = i
	pq[j].index = j
}

func (pq priority_queue) Less(i, j int) bool { 
	return pq[i].proposed_order < pq[j].proposed_order 
}

func (pq priority_queue) update(x transcation, proposed_order float64) {
	if !contains(x, pq){
		return
	}
	x.proposed_order = proposed_order
	heap.Fix(pq, x.index)
}

func (pq priority_queue) remove(x transcation) {
	if !contains(x, pq){
		return
	}
	heap.Remove(pq, x.index)
}
