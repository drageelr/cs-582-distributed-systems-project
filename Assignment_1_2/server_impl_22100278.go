// Implementation of a KeyValueServer. Students should write their code in this file.

package pa1

import (
	"DS_PA1/rpcs"
	"bufio"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
)

type keyValueServer struct {
	clients     map[int]*connection
	putReq      chan reqData
	getReq      chan reqData
	outReq      chan reqData
	getCountReq chan bool
	count       chan int
	newConn     chan net.Conn
	delConnec   chan int
	exit        bool
}

type connection struct {
	conn     net.Conn
	reader   *bufio.Reader
	writer   *bufio.Writer
	buffchan chan reqData
}

type reqData struct {
	key string
	val []byte
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	kvs := keyValueServer{make(map[int]*connection), make(chan reqData), make(chan reqData), make(chan reqData), make(chan bool), make(chan int), make(chan net.Conn), make(chan int), false}
	return &kvs
}

func (kvs *keyValueServer) StartModel1(port int) error {
	// Initialize DB
	initDB()

	// Start Listener
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err == nil {
		/*
			If no error in starting the listener then
				Pass the listener object and kvs pointer to a new go routine
				This go routine will continue listening on this listener
		*/
		go func(ln net.Listener, kvs *keyValueServer) {
			/*
				Start 2 go routines once
					handleKVS: Handles everything which requries accessing the kvs struct
					handleRequest: Handles everything which requires accessing the DB
			*/
			go kvs.handleKVS()
			go kvs.handleRequest()

			// Iterate over the listener
			for {
				conn, err := ln.Accept()

				if err == nil {
					// If no error in accepting a new connection then pass the connection object to a channel
					kvs.newConn <- conn
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
	// Send request for getting count by adding true in channel "getCountReq"
	kvs.getCountReq <- true
	// Return result for the count by reading from the channel "count"
	return <-kvs.count
}

func (kvs *keyValueServer) StartModel2(port int) error {
	// Initialize DB
	initDB()

	// Start go routine handleRequest: Handles everything which requires accessing the DB
	go kvs.handleRequest()

	// Start Listener
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err == nil {
		// If no error then start the RPC server
		rpcServer := rpc.NewServer()
		rpcServer.Register(rpcs.Wrap(kvs))
		http.DefaultServeMux = http.NewServeMux()
		rpcServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
		go http.Serve(ln, nil)
	}
	return err
}

func (kvs *keyValueServer) RecvGet(args *rpcs.GetArgs, reply *rpcs.GetReply) error {
	// Add get request to "getReq" channel
	kvs.getReq <- reqData{args.Key, []byte("")}

	// Read output from "outReq" channel
	outputData := <-kvs.outReq

	// Respond with the result
	reply.Value = outputData.val
	return nil
}

func (kvs *keyValueServer) RecvPut(args *rpcs.PutArgs, reply *rpcs.PutReply) error {
	// Add put request to "putReq" channel
	kvs.putReq <- reqData{args.Key, args.Value}
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

// Handles requests relating to kvs struct
func (kvs *keyValueServer) handleKVS() {
	id := 0
	for {
		select {

		// If get count request recieved then return the count of clients
		case <-kvs.getCountReq:
			kvs.count <- len(kvs.clients)

			// If new connection object recieved then initialize the new connection and start relevant go routines for that client
		case conn := <-kvs.newConn:
			// Initializing new connection and relevant data structures
			connec := new(connection)
			connec.conn = conn
			connec.reader = bufio.NewReader(conn)
			connec.writer = bufio.NewWriter(conn)
			connec.buffchan = make(chan reqData, 500)
			kvs.clients[id] = connec

			// Start go routine for handling incoming "put" or "get" requests from clients
			go kvs.handleConnection(connec.reader, id)

			// Start go routine for handling output to the client
			go kvs.handleResponse(connec.writer, connec.buffchan)

			id++

			// If delete request is made then delete that client's data structures
		case idDel := <-kvs.delConnec:
			delete(kvs.clients, idDel)

			// If any output is recieved from the DB through a channel by "HandleRequest" function then add the output to every client's output channel
		case outReqData := <-kvs.outReq:
			for _, connec := range kvs.clients {
				if len(connec.buffchan) != cap(connec.buffchan) {
					connec.buffchan <- outReqData
				}
			}
		}
	}
}

// Handles incoming "put" or "get" requests from a client
func (kvs *keyValueServer) handleConnection(reader *bufio.Reader, id int) {
	for !kvs.exit {
		// Read msg from client
		msg, err := reader.ReadString('\n')
		if err != nil {
			// If error reading then terminate connection and end go routine
			kvs.delConnec <- id
			return
		}

		if msg[0:3] == "put" {
			// If put request recieved then pass data to "putReq" channel
			r1, r2 := getKeyVal(msg[4:])
			putReqData := reqData{r1, []byte(r2)}
			kvs.putReq <- putReqData
		} else if msg[0:3] == "get" {
			// If get request recieved then pass data to "getReq" channel
			getReqData := reqData{msg[4 : len(msg)-1], make([]byte, 0)}
			kvs.getReq <- getReqData
		}
	}
}

// Handles requests which require access to DB
func (kvs *keyValueServer) handleRequest() {
	for !kvs.exit {
		select {

		// If get request recieved then return the requested key to "outReq" channel
		case getReqData := <-kvs.getReq:
			getReqData.val = get(getReqData.key)
			kvs.outReq <- getReqData

		// If put request recieved then add the data to DB
		case putReqData := <-kvs.putReq:
			put(putReqData.key, putReqData.val)
		}
	}
}

// Handles output for a client
func (kvs *keyValueServer) handleResponse(writer *bufio.Writer, buffchan chan reqData) {
	for {
		// Read the client's output channel
		outputData := <-buffchan

		// Convert the output to a string
		outString := outputData.key + "," + string(outputData.val) + "\n"

		// Send output string to the client
		writer.WriteString(outString)
		writer.Flush()
	}
}
