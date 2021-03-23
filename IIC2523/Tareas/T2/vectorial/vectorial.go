package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"

	"../comm"
	"../ins"
)

const retriesPerHost int = 10

const connType = "tcp"

var wgStart sync.WaitGroup
var wgEnd sync.WaitGroup
var wgQueue sync.WaitGroup

var messageMutex sync.Mutex

var vectorialClock []int

var messageArray []Tuple
var msgIDCounter int = 0

// Message ...
type Message struct {
	Command string
	Value   string
	Clock   []int
}

// Tuple ...
type Tuple struct {
	msg      Message
	senderID int
	msgID    int
}

func (m Message) String() string {
	return fmt.Sprintf("\tCMD:\t%v\n\tVAL:\t%v\n\tCLK:\t%v", m.Command, m.Value, m.Clock)
}

func check(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func updateClock(timestamp []int) {
	for i := range timestamp {
		// get max
		if timestamp[i] > vectorialClock[i] {
			vectorialClock[i] = timestamp[i]
		}
	}
}

func verifyClock(localHost *comm.Client, clock []int, senderID int) bool {
	i := senderID
	// COND 1
	if clock[i] != vectorialClock[i]+1 {
		return false
	}
	// COND 2
	for k := range clock {
		if k == i {
			continue
		}
		if clock[k] > vectorialClock[k] {
			return false
		}
	}
	return true
}

func executeMessagesFromArray(localHost *comm.Client) {
	messageExecuted := true
	for len(messageArray) > 0 && messageExecuted {
		messageExecuted = false
		for i := range messageArray {
			// Found a valid message!
			tup := messageArray[i]
			if verifyClock(localHost, tup.msg.Clock, tup.senderID) {
				// Pop from queue
				tup := messageArray[i]
				messageArray = append(messageArray[i:], messageArray[i+1:]...)
				wgQueue.Done()
				// Execute message
				fmt.Printf("ID %d - Executing message previously received from host %d with message ID %d...\n", localHost.ID, tup.senderID, tup.msgID)
				updateClock(tup.msg.Clock)
				messageExecuted = true
				// reset loop
				break
			}
		}
	}
}

func acceptConnections(localHost *comm.Client, otherHosts []*comm.Host) {
	fmt.Printf("ID %d - Host %v accepting connections\n", localHost.ID, localHost)
	for i := 0; i < len(otherHosts); {
		clientSocket, err := localHost.ListenSocket.Accept()
		check(err)

		// Receive address
		message, err := receiveMessage(clientSocket)
		address := message.Value

		// Find corresponding host
		host := comm.FindHostByAddress(address, otherHosts)
		if host == nil {
			// If unrecognized host, skip iteration
			fmt.Printf("ID %d - Warning: Accepted connection from unknown host\n", localHost.ID)
			clientSocket.Close()
		} else {
			host.ClientSocket = clientSocket
			host.IsActive = true
			fmt.Printf("ID %d - Accepted connection from %v\n", localHost.ID, host)
			// Start listening
			go func() {
				listenClient(localHost, host, otherHosts)
			}()
			i++
		}
	}
	fmt.Printf("ID %d - %v successfully accepted all connections\n", localHost.ID, localHost)
}

func listenClient(localHost *comm.Client, host *comm.Host, otherHosts []*comm.Host) {
	for {
		message, err := receiveMessage(host.ClientSocket)
		if err != nil {
			fmt.Printf("ID %d - Connection with %v ended. Closing sockets...\n", localHost.ID, host)
			host.ClientSocket.Close()
			break
		}
		// ACQUIRE MUTEX HERE
		messageMutex.Lock()
		go handleMessage(message, localHost, host, otherHosts)
		// RELEASE MUTEX HERE
		messageMutex.Unlock()
	}
}

func handleMessage(msg Message, localHost *comm.Client, host *comm.Host, otherHosts []*comm.Host) {
	command := msg.Command
	fmt.Printf("ID %d - Received message from %v:\n%v\n", localHost.ID, host, msg)

	switch command {
	case "START":
		wgStart.Done()
	case "END":
		wgEnd.Done()
	case "MSG":
		fmt.Printf("ID %d - CLOCK VALUE BEFORE: %v\n", localHost.ID, vectorialClock)
		// Push message to queue (id counter minus extra minus self)
		// Sort queue
		// TODO: FIX
		timestamp := msg.Clock
		isValid := verifyClock(localHost, timestamp, host.ID)
		if isValid {
			// Deliver message to app and update clock
			fmt.Printf("ID %d - Executing message...\n", localHost.ID)
			updateClock(timestamp)
			executeMessagesFromArray(localHost)
		} else {
			// Push to queue
			fmt.Printf("ID %d - Can't execute, adding to queue for later...\n", localHost.ID)
			wgQueue.Add(1)
			messageArray = append(messageArray, Tuple{msg, host.ID, msgIDCounter})
			msgIDCounter++
		}
		fmt.Printf("ID %d - CLOCK VALUE AFTER: %v\n", localHost.ID, vectorialClock)
	}
}

func connectToHosts(localHost *comm.Client, otherHosts []*comm.Host) {
	fmt.Printf("ID %d - Host %v connecting to other hosts\n", localHost.ID, localHost)
	currentRetries := 0
	for i := 0; i < len(otherHosts); {
		listenSocket, err := net.Dial(connType, fmt.Sprint(otherHosts[i]))
		if err != nil {
			// Connection failed
			if currentRetries > retriesPerHost {
				check(err)
			}
			fmt.Printf("ID %v - Can't connect to %v. Retrying...\n", localHost.ID, otherHosts[i])
			currentRetries++
		} else {
			// Connection successful
			currentRetries = 0
			// Add connetion to corresponding host
			address := listenSocket.RemoteAddr().String()
			host := comm.FindHostByAddress(address, otherHosts)
			host.ListenSocket = listenSocket
			fmt.Printf("ID %v - Successfully connected to %v\n", localHost.ID, host)
			// Send address
			msg := Message{"ADDRESS", fmt.Sprint(localHost), vectorialClock}
			fmt.Printf("ID %d - Sending message to %v:\n%v\n", localHost.ID, host, msg)
			sendMessage(host.ListenSocket, msg)
			// Try next host
			i++
		}
	}
	fmt.Printf("ID %d - %v successfully connected to all other hosts\n", localHost.ID, localHost)
}

func receiveMessage(socket net.Conn) (Message, error) {
	buffer, err := bufio.NewReader(socket).ReadBytes('\x00')
	var msg Message
	if len(buffer) != 0 {
		json.Unmarshal(buffer[:len(buffer)-1], &msg)
	}
	return msg, err
}

func sendMessage(socket net.Conn, msg Message) {
	b, err := json.Marshal(msg)
	check(err)
	b = append(b, byte('\x00'))
	socket.Write(b)
}

func executeInstruction(localHost *comm.Client, otherHosts []*comm.Host, instruction ins.Instruction) {
	msgIns, okMsg := instruction.(ins.MessageInstruction)
	mulIns, okMul := instruction.(ins.MulticastInstruction)
	incIns, okInc := instruction.(ins.IncrementInstruction)
	if okMsg {
		if msgIns.SenderID != localHost.ID {
			fmt.Printf("ID %d - Skipping instruction %v...\n", localHost.ID, msgIns)
			return
		}
		fmt.Printf("ID %d - RUNNING INSTRUCTION: %v\n", localHost.ID, msgIns)
		fmt.Printf("ID %d - CLOCK VALUE BEFORE: %v\n", localHost.ID, vectorialClock)
		vectorialClock[localHost.ID]++
		target := comm.FindHostByID(msgIns.TargetID, otherHosts)
		msg := Message{"MSG", "MSG", vectorialClock}
		fmt.Printf("ID %d - Sending message to %v:\n%v\n", localHost.ID, target, msg)
		sendMessage(target.ListenSocket, msg)
		fmt.Printf("ID %d - CLOCK VALUE AFTER: %v\n", localHost.ID, vectorialClock)
	} else if okMul {
		if mulIns.SenderID != localHost.ID {
			fmt.Printf("ID %d - Skipping instruction %v...\n", localHost.ID, mulIns)
			return
		}
		fmt.Printf("ID %d - RUNNING INSTRUCTION: %v\n", localHost.ID, mulIns)
		fmt.Printf("ID %d - CLOCK VALUE BEFORE: %v\n", localHost.ID, vectorialClock)
		vectorialClock[localHost.ID]++
		for _, targetID := range mulIns.TargetIDs {
			target := comm.FindHostByID(targetID, otherHosts)
			msg := Message{"MSG", "MSG", vectorialClock}
			fmt.Printf("ID %d - Sending message to %v:\n%v\n", localHost.ID, target, msg)
			sendMessage(target.ListenSocket, msg)
		}
		fmt.Printf("ID %d - CLOCK VALUE AFTER: %v\n", localHost.ID, vectorialClock)
	} else if okInc {
		// Esta línea ignora la instrucción
		fmt.Printf("ID %d - Skipping instruction %v...\n", localHost.ID, incIns)
		// Estas lineas aumentan el reloj correspondiente
		// fmt.Printf("ID %d - RUNNING INSTRUCTION: %v\n", localHost.ID, incIns)
		// fmt.Printf("ID %d - CLOCK VALUE BEFORE: %v\n", localHost.ID, vectorialClock)
		// fmt.Printf("ID %d - Incrementing clock by %v...\n", localHost.ID, incIns.Increment)
		// vectorialClock[localHost.ID] += incIns.Increment
		// fmt.Printf("ID %d - CLOCK VALUE AFTER: %v\n", localHost.ID, vectorialClock)
	}
}

func main() {
	// Retrieve command-line arguments
	if cap(os.Args) != 4 {
		fmt.Println("Usage: .\\program.go <config_file> <id> <instruction_file>")
		os.Exit(1)
	}
	// Get ID
	id, err := strconv.Atoi(os.Args[2])
	check(err)
	// Get hosts
	hostArray := comm.GetHosts(os.Args[1])
	otherHosts := make([]*comm.Host, len(hostArray)-1)
	// Get corresponding host based on ID
	var localHost *comm.Client = nil
	for i := range hostArray {
		if hostArray[i].ID == id {
			// Set localHost
			temp := comm.HostToClient(hostArray[i])
			localHost = temp
			// Remove from other array
			copy(otherHosts[:i], hostArray[:i])
			copy(otherHosts[i:], hostArray[i+1:])
			break
		}
	}
	// Invalid host (id not in file)
	if localHost == nil {
		fmt.Printf("ID %d - Error: Could not find host configuration for id %d\n", id, id)
		os.Exit(1)
	}

	// Initialize vectorial clock
	vectorialClock = make([]int, len(otherHosts)+1)
	// Start process communication
	wgStart.Add(len(otherHosts))
	wgEnd.Add(len(otherHosts))
	fmt.Printf("ID %d - Starting process with address %v\n", localHost.ID, localHost)

	// Create listen socket
	listenSocket, err := net.Listen(connType, fmt.Sprint(localHost))
	check(err)
	localHost.ListenSocket = listenSocket

	// Accept other hosts
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		acceptConnections(localHost, otherHosts)
		wg.Done()
	}()

	go func() {
		connectToHosts(localHost, otherHosts)
		wg.Done()
	}()

	// Get instructions
	fmt.Printf("ID %d - Parsing instructions...\n", localHost.ID)
	instructionArray := ins.ReadInstructions(os.Args[3], comm.IDCounter-1)
	fmt.Printf("ID %d - Instructions parsed!\n", localHost.ID)

	wg.Wait()

	// Done processing instructions and establishing connections
	// Send START message
	for _, host := range otherHosts {
		msg := Message{"START", "START", vectorialClock}
		fmt.Printf("ID %d - Sending message to %v:\n%v\n", localHost.ID, host, msg)
		sendMessage(host.ListenSocket, msg)
	}

	wgStart.Wait()

	fmt.Printf("-------------------------------------------------------------\n")
	fmt.Printf("ID %d - Everyone's ready! Starting instructions...\n", localHost.ID)
	fmt.Printf("-------------------------------------------------------------\n")

	// Execute instructions
	for i := range instructionArray {

		messageMutex.Lock()
		executeInstruction(localHost, otherHosts, instructionArray[i])
		messageMutex.Unlock()
	}

	// Send END message

	// time.Sleep(time.Second * 10)
	fmt.Printf("ID %d - Instructions done. Waiting for other processes...\n", localHost.ID)

	wgQueue.Wait()

	for _, host := range otherHosts {
		if host.IsActive {
			msg := Message{"END", "END", vectorialClock}
			fmt.Printf("ID %d - Sending message to %v: \n %v \n", localHost.ID, host, msg)
			sendMessage(host.ListenSocket, msg)
		}
	}

	wgEnd.Wait()
	wgQueue.Wait()
	fmt.Printf("ID %d - All processes done. Terminating program...\n", localHost.ID)
	// Close all sockets
	localHost.ListenSocket.Close()
	for _, host := range otherHosts {
		host.ListenSocket.Close()
		host.ClientSocket.Close()
	}
	fmt.Printf("ID %d - FINAL CLOCK: %v\n", localHost.ID, vectorialClock)
}
