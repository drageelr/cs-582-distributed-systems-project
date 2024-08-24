// Implementation of a KeyValueServer. Students should write their code in this file.

package pa1

import (
	"DS_PA1/rpcs"
	"bufio"
	"net"
	"strconv"
	// "fmt"
)

type keyValueServer struct {
	// TODO: implement this!
	clients map[int]*connection
	putReq  chan reqData
	getReq  chan reqData
	outReq  chan reqData
	exit    bool
}

type connection struct {
	conn net.Conn
	// rw   *bufio.ReadWriter
	reader *bufio.Reader
	writer *bufio.Writer
	exit   bool
}

type reqData struct {
	key string
	val []byte
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	// TODO: implement this!
	kvs := keyValueServer{make(map[int]*connection), make(chan reqData), make(chan reqData), make(chan reqData), false}
	return &kvs
}

func (kvs *keyValueServer) StartModel1(port int) error {
	// TODO: implement this!
	initDB()
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err == nil {
		go func(ln net.Listener, kvs *keyValueServer) {
			id := 0
			go handleRequest(kvs)
			go handleBroadcast(kvs)
			for {
				conn, err := ln.Accept()

				if err == nil {
					connec := new(connection)
					connec.conn = conn
					// connec.rw = bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
					connec.reader = bufio.NewReader(conn)
					connec.writer = bufio.NewWriter(conn)
					connec.exit = false
					kvs.clients[id] = connec
					go handleConnection(id, kvs)
					id++
				} else {
					// fmt.Printf("Couldn't accept a client connection: %s\n", err)
				}
			}
		}(ln, kvs)
	}
	return err
}

func (kvs *keyValueServer) Close() {
	// TODO: implement this!
}

func (kvs *keyValueServer) Count() int {
	// TODO: implement this!
	return len(kvs.clients)
}

func (kvs *keyValueServer) StartModel2(port int) error {
	// TODO: implement this!
	//
	// Do not forget to call rpcs.Wrap(...) on your kvs struct before
	// passing it to <sv>.Register(...)
	//
	// Wrap ensures that only the desired methods (RecvGet and RecvPut)
	// are available for RPC access. Other KeyValueServer functions
	// such as Close(), StartModel1(), etc. are forbidden for RPCs.
	//
	// Example: <sv>.Register(rpcs.Wrap(kvs))
	return nil
}

func (kvs *keyValueServer) RecvGet(args *rpcs.GetArgs, reply *rpcs.GetReply) error {
	// TODO: implement this!
	return nil
}

func (kvs *keyValueServer) RecvPut(args *rpcs.PutArgs, reply *rpcs.PutReply) error {
	// TODO: implement this!
	return nil
}

// TODO: add additional methods/functions below!
func getKeyVal(x string) (string, string) {
	var r1, r2 string
	for i := 0; i < len(x); i++ {
		if x[i] == []byte(",")[0] {
			r1 = x[0:i]
			r2 = x[i+1 : len(x)-1]
		}
	}
	return r1, r2
}

func handleConnection(id int, kvs *keyValueServer) {
	connec := kvs.clients[id]
	for !connec.exit && !kvs.exit {
		msg, err := connec.reader.ReadString('\n')
		if err != nil {
			// fmt.Printf("Error reading message from client_%d: %s\n", id, err)
			return
		}
		// fmt.Printf("Msg recieved from client_%d: %s", id, msg)
		if msg[0:3] == "put" {
			r1, r2 := getKeyVal(msg[4:])
			putReqData := reqData{r1, []byte(r2)}
			kvs.putReq <- putReqData
			// fmt.Printf("Put Queued client_%d\n", id)
		} else if msg[0:3] == "get" {
			getReqData := reqData{msg[4 : len(msg)-1], make([]byte, 0)}
			kvs.getReq <- getReqData
			// fmt.Printf("Get Queued client_%d\n", id)
		} else {
			// fmt.Printf("Invalid command sent by client_%d: %s\n", id, msg)
		}
	}
}

func handleRequest(kvs *keyValueServer) {
	for !kvs.exit {
		select {
		case getReqData := <-kvs.getReq:
			// fmt.Printf("Get UnQueued\n")
			getReqData.val = get(getReqData.key)
			kvs.outReq <- getReqData
			// fmt.Printf("Out Queued\n")
		case putReqData := <-kvs.putReq:
			// fmt.Printf("Put UnQueued\n")
			put(putReqData.key, putReqData.val)
		}
	}
}

func handleBroadcast(kvs *keyValueServer) {
	for !kvs.exit {
		outReqData := <-kvs.outReq
		//fmt.Printf("Out UnQueued\n")
		outString := outReqData.key + "," + string(outReqData.val) + "\n"
		for _, connec := range kvs.clients {
			connec.writer.WriteString(outString)
			err := connec.writer.Flush()
			if err != nil {
				// fmt.Printf("Eror sending to client_%d: %s", id, outString)
			} else {
				// fmt.Printf("Sending to client_%d: %s", id, outString)
			}

		}
	}
}
