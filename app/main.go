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
	value     string
	expiresAt int64
}

var (
	store     = make(map[string]entry)
	listStore = make(map[string][]string)
	mu        sync.RWMutex
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

	for {
		args := handleReader(conn)
		if args == nil || len(args) == 0 {
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
		case "RPUSH":
			handleRPUSH(args, conn)
		case "LRANGE":
			handleLRANGE(args, conn)
		case "LPUSH":
			handleLPUSH(args, conn)
		case "LLEN":
			handleLLEN(args, conn)
		case "LPOP":
			handleLPOP(args, conn)
		default:
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}

// func handleLPOP(args []string, conn net.Conn) {
	
// 	if len(args) < 2 || len(args) > 3 {
// 		conn.Write([]byte("-ERR wrong number of arguments for 'LPOP'\r\n"))
// 		return
// 	}

// 	listName := args[1]
	

// 	if len(args) == 2 {
// 		mu.Lock()
// 		defer mu.Unlock()
// 		 list, exists := listStore[listName]
// 		 if !exists || len(list) == 0 {
// 			conn.Write([]byte("$-1\r\n"))
// 		 }
// 		 popped := list[0]
// 		 listStore[listName] = list[1:]

// 	}
// 	valtoRemove := 1
// 	var err error

// 	if len(args) == 3 {
// 		valtoRemove, err = strconv.Atoi(args[2])
// 	}
	
// 	if err != nil  {
// 		conn.Write([]byte("-ERR invalid range to remove\r\n"))
// 		return
// 	}

// 	mu.Lock()

// 	if _, exists := listStore[listName]; !exists {
// 		conn.Write([]byte("$-1\r\n"))
// 		mu.Unlock()
// 		return
// 	} else {
// 			if valtoRemove > len(listStore[listName]) {
// 			var arrResp []string
// 			lenght := len(listStore[listName])

// 			for i := 0; i < lenght; i++ {
// 				popped := listStore[listName][0]
// 				listStore[listName] = listStore[listName][1:]
// 				arrResp = append(arrResp, popped)
// 			}
			
// 			if len(listStore[listName]) == 0 {
// 				delete(listStore, listName)
// 			}

// 			var sb strings.Builder
// 			sb.WriteString(fmt.Sprintf("*%d\r\n", len(arrResp)))
// 			for _, elem := range arrResp {
// 				sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(elem), elem))
// 			}
// 			finalRespString := sb.String()
// 			conn.Write([]byte(finalRespString))

// 			mu.Unlock()
// 			return
// 		} else {
// 		// arrResp := make([]string, len(valtoRemove))
// 			var arrResp []string
// 			for i := 0; i < valtoRemove; i++ {
// 				popped := listStore[listName][0]
// 				listStore[listName] = listStore[listName][1:]
// 				arrResp = append(arrResp, popped)
// 			}

// 			var sb strings.Builder
// 			sb.WriteString(fmt.Sprintf("*%d\r\n", len(arrResp)))
// 			for _, elem := range arrResp {
// 				sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(elem), elem))
// 			}
// 			finalRespString := sb.String()
// 			conn.Write([]byte(finalRespString))
// 			mu.Unlock()
// 			return
// 		}
// 	}
// }

func handleLPOP(args []string, conn net.Conn) {

	if len(args) < 2 || len(args) > 3 {
		conn.Write([]byte("-ERR wrong number of arguments for 'LPOP'\r\n"))
		return
	}

	listName := args[1]
	
    // FIX: This entire block handles the simple "LPOP key" case
	if len(args) == 2 {
		mu.Lock()
		defer mu.Unlock()

		list, exists := listStore[listName]
		if !exists || len(list) == 0 {
			conn.Write([]byte("$-1\r\n")) // Non-existent or empty list
			return
		}

		// Pop the first element
		popped := list[0]
		listStore[listName] = list[1:]

		// If the list is now empty, remove the key
		if len(listStore[listName]) == 0 {
			delete(listStore, listName)
		}

		// Respond with a single BULK STRING, not an array
		resp := fmt.Sprintf("$%d\r\n%s\r\n", len(popped), popped)
		conn.Write([]byte(resp))
		return
	}

    // FIX: This entire block handles the "LPOP key count" case.
    // Your previous logic was mostly correct for this, so we keep it here.
	if len(args) == 3 {
		valtoRemove, err := strconv.Atoi(args[2])
		if err != nil {
			conn.Write([]byte("-ERR invalid range to remove\r\n"))
			return
		}

		mu.Lock()
		defer mu.Unlock()

		if _, exists := listStore[listName]; !exists || len(listStore[listName]) == 0 {
			conn.Write([]byte("*0\r\n")) // For a count, return an empty array if key doesn't exist
			return
		}
		
        // Determine the actual number of items to pop
		numToPop := valtoRemove
		if numToPop > len(listStore[listName]) {
			numToPop = len(listStore[listName])
		}

		var arrResp []string
		for i := 0; i < numToPop; i++ {
			popped := listStore[listName][0]
			listStore[listName] = listStore[listName][1:]
			arrResp = append(arrResp, popped)
		}
		
		if len(listStore[listName]) == 0 {
			delete(listStore, listName)
		}

        // Build and send the ARRAY response
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("*%d\r\n", len(arrResp)))
		for _, elem := range arrResp {
			sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(elem), elem))
		}
		finalRespString := sb.String()
		conn.Write([]byte(finalRespString))
		return
	}
}

func handleLLEN(args []string, conn net.Conn) {
	if len(args) < 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'LLEN'\r\n"))
		return
	}

	listName := args[1]

	mu.Lock()
	if _, exists := listStore[listName]; !exists {
		conn.Write([]byte(fmt.Sprintf(":%d\r\n", 0)))
		mu.Unlock()
		return
	} else {
		length := len(listStore[listName])
		conn.Write([]byte(fmt.Sprintf(":%d\r\n", length)))
		mu.Unlock()
		return
	}
}

func handleLPUSH(args []string, conn net.Conn) {
	if len(args) < 3 {
		conn.Write([]byte("-ERR wrong number of arguments for 'LPUSH'\r\n"))
		return
	}

	listName := args[1]
	values := args[2:]

	mu.Lock()
	if _, exists := listStore[listName]; !exists {
		listStore[listName] = make([]string, 0)
		listStore[listName] = append(listStore[listName], values...)
		elements := len(listStore[listName])
		conn.Write([]byte(fmt.Sprintf(":%d\r\n", elements)))
		mu.Unlock()
		return
	} else {
		for _, value := range values {
			listStore[listName] = append([]string{value}, listStore[listName]...)
		}
		elements := len(listStore[listName])
		conn.Write([]byte(fmt.Sprintf(":%d\r\n", elements)))
		mu.Unlock()
		return
	}
}

func handleLRANGE(args []string, conn net.Conn) {
	if len(args) != 4 {
		conn.Write([]byte("-ERR wrong number of arguments for 'LRANGE'"))
		return
	}

	listName := args[1]
	start, err1 := strconv.Atoi(args[2])
	stop, err2 := strconv.Atoi(args[3])

	fmt.Println("args start stop", start, stop)

	if err1 != nil || err2 != nil {
		conn.Write([]byte("-ERR invalid index\r\n"))
		return
	}

	mu.RLock()
	list, exists := listStore[listName]
	mu.RUnlock()

	//list does not extis
	if !exists {
		conn.Write([]byte("*0\r\n"))
		return
	}

	if start >= len(list) {
		conn.Write([]byte("*0\r\n"))
		return
	}

	if stop >= len(list) {
		stop = len(list) - 1
	}

	if start < 0 {
		start = max(len(list)+start, 0)
	}

	if stop < 0 {
		stop = max(len(list)+stop, 0)
	}

	elements := list[start : stop+1]

	fmt.Println("idx", start, stop)
	fmt.Println("ele idx", list[start], list[stop])
	fmt.Println("elements", elements)

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(elements)))

	for _, elem := range elements {
		sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(elem), elem))
	}

	conn.Write([]byte(sb.String()))
}

func handleRPUSH(args []string, conn net.Conn) {
	if len(args) < 3 {
		conn.Write([]byte("-ERR wrong number of arguments for 'RPUSH'\r\n"))
		return
	}

	listName := args[1]
	values := args[2:]

	mu.Lock()
	if _, exists := listStore[listName]; !exists {
		listStore[listName] = make([]string, 0)
		listStore[listName] = append(listStore[listName], values...)
		elements := len(listStore[listName])
		conn.Write([]byte(fmt.Sprintf(":%d\r\n", elements)))
		mu.Unlock()
		return
	} else {
		for _, value := range values {
			listStore[listName] = append(listStore[listName], value)
		}
		elements := len(listStore[listName])
		conn.Write([]byte(fmt.Sprintf(":%d\r\n", elements)))
		mu.Unlock()
		return
	}
}

func handleSet(args []string, conn net.Conn) {

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

func handleGET(args []string, conn net.Conn) {
	if len(args) < 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'GET'\r\n"))
		return
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

	// Return bulk string
	resp := fmt.Sprintf("$%d\r\n%s\r\n", len(e.value), e.value)
	conn.Write([]byte(resp))
}

func handleEcho(args []string, conn net.Conn) {
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
