package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	
	}
}

func handleConnection(conn net.Conn) {
	var (
		store = make(map[string]string)
		mu sync.RWMutex
	)

	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		// Read array header (*N\r\n)
		line, err := reader.ReadString('\n')
		if err != nil {
			return // client disconnected or error
		}

		line = strings.TrimSuffix(line, "\r\n")
		if len(line) == 0 || line[0] != '*' {
			return // protocol error or close
		}

		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return
		}

		args := make([]string, 0, n)

		for i := 0; i < n; i++ {
			bulkLenLine, err := reader.ReadString('\n')
			if err != nil {
				return
			}

			bulkLenLine = strings.TrimSuffix(bulkLenLine, "\r\n")
			if len(bulkLenLine) == 0 || bulkLenLine[0] != '$' {
				return
			}

			bulkLen, err := strconv.Atoi(bulkLenLine[1:])
			if err != nil || bulkLen < 0 {
				return
			}

			bulk := make([]byte, bulkLen)
			_, err = io.ReadFull(reader, bulk)
			if err != nil {
				return
			}

			if _, err := reader.Discard(2); err != nil {
				return
			}

			args = append(args, string(bulk))
		}

		if len(args) == 0 {
			continue
		}

		cmd := strings.ToUpper(args[0])

		switch cmd {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			if len(args) >= 2 {
				msg := args[1]
				resp := fmt.Sprintf("$%d\r\n%s\r\n", len(msg), msg)
				conn.Write([]byte(resp))
			} else {
				conn.Write([]byte("$0\r\n\r\n"))
			}
		case "SET":
			if len(args) >= 3 {
				key := args[1]
				value := args[2]

				mu.Lock()
				store[key] = value
				mu.Unlock()
				
				conn.Write([]byte("+OK\r\n"))
			} else {
				conn.Write([]byte("-ERR wrong numebr of arguments for 'SET'\r\n"))
			}
		case "GET":
			if len(args) >= 2 {
				key := args[1]

				mu.RLock()
				value, exists := store[key]
				mu.RUnlock()

				if exists {
					resp := fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
					conn.Write([]byte(resp))
				} else {
					conn.Write([]byte("-ERR wrong number of arguments for 'GET'"))
				}
			}

		default:
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}