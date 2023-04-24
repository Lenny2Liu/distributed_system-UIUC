# Makefile for building server.go and client.go

# Go compiler
GO = go

# Output file names
SERVER_OUT = server
CLIENT_OUT = client

# Source file names
SERVER_SRC = server.go
CLIENT_SRC = client.go

all: server client

server:
	$(GO) build -o $(SERVER_OUT) $(SERVER_SRC)

client:
	$(GO) build -o $(CLIENT_OUT) $(CLIENT_SRC)

clean:
	rm -f $(SERVER_OUT) $(CLIENT_OUT)

.PHONY: all server client clean
