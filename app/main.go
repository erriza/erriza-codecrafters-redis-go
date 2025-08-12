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
	"time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit
type entry struct {
	value 		string
	expiresAt 	int64
}
var (
		store = make(map[string]entry)
		mu sync.RWMutex
)

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

	defer conn.Close()

	args := handleReader(conn)
	if len(args) == 0 {
		return
	}

	cmd := strings.ToUpper(args[0])

	switch cmd {
	case "PING":
		conn.Write([]byte("+PONG\r\n"))
	case "ECHO":
		handleEcho(args, conn)
	case "SET":
		handleSet(args, conn)
	case "GET":
		handleGET(args, conn)
	default:
		conn.Write([]byte("-ERR unknown command\r\n"))
	}
}

func handleSet(args []string, conn net.Conn)  {

	if len(args) < 3 {
		conn.Write([]byte("-ERR wrong numebr of arguments for 'SET'\r\n"))
		return
	}

	key := args[1]
	value := args[2]
	var expiresAt int64

	if len(args) >= 5 && strings.ToUpper(args[3]) == "PX" {
		ms, err := strconv.Atoi(args[4])
		if err != nil || ms < 0 {
			conn.Write([]byte("-ERR invalid PC value\r\n"))
			return
		}
		expiresAt = time.Now().UnixMilli() + int64(ms)
	}

	mu.Lock()
	store[key] = entry{value: value, expiresAt: expiresAt}
	mu.Unlock()
		
	conn.Write([]byte("+OK\r\n"))
	
}

func handleGET (args []string, conn net.Conn) {
	if len(args) < 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'GET'"))
	}
	
	key := args[1]
	mu.RLock()
	e, exists := store[key]
	mu.RUnlock()

	if !exists {
		conn.Write([]byte("$-1\r\n"))
		return
	}

	if e.expiresAt > 0 && time.Now().UnixMilli() > e.expiresAt {
		mu.Lock()
		delete(store, key)
		mu.Unlock()
		conn.Write([]byte("$-1\r\n"))
		return
	}
	resp := fmt.Sprintf("$%d\r\n%s\r\n", len(e.value), e.value)
	conn.Write([]byte(resp))
	
}

func handleEcho (args []string, conn net.Conn) {
	if len(args) >= 2 {
		msg := args[1]
		resp := fmt.Sprintf("$%d\r\n%s\r\n", len(msg), msg)
		conn.Write([]byte(resp))
	} else {
		conn.Write([]byte("$0\r\n\r\n"))
	}
}

func handleReader(conn net.Conn) []string {
	reader := bufio.NewReader(conn)

	// Read array header (*N\r\n)
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil
	}

	line = strings.TrimSuffix(line, "\r\n")
	if len(line) == 0 || line[0] != '*' {
		return nil
	}

	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil
	}

	args := make([]string, 0, n)

	for i := 0; i < n; i++ {
		bulkLenLine, err := reader.ReadString('\n')
		if err != nil {
			return nil
		}

		bulkLenLine = strings.TrimSuffix(bulkLenLine, "\r\n")
		if len(bulkLenLine) == 0 || bulkLenLine[0] != '$' {
			return nil
		}

		bulkLen, err := strconv.Atoi(bulkLenLine[1:])
		if err != nil || bulkLen < 0 {
			return nil
		}

		bulk := make([]byte, bulkLen)
		_, err = io.ReadFull(reader, bulk)
		if err != nil {
			return nil
		}

		if _, err := reader.Discard(2); err != nil { // discard \r\n
			return nil
		}

		args = append(args, string(bulk))
	}

	return args
}