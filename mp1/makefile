# Makefile

all: mp1_node

mp1_node: mp1.transaction.go priorityqueue.go
	go build -o mp1_node mp1.transaction.go priorityqueue.go

clean:
	rm -f mp1_node
