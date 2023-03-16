all: logger node

logger: logger.go
	go build logger.go

node: node.go
	go build node.go

clean:
	rm -f logger node
