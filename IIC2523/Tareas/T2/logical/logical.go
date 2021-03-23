package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"

	"../comm"
	"../ins"
)

const retriesPerHost int = 10

const connType = "tcp"

var wgStart sync.WaitGroup
var wgEnd sync.WaitGroup

var messageMutex sync.Mutex

var logicClock int = 0

var ackCounter int = 0
var msgIDCounter int = 0
var messageQueue []Tuple

var queueSignalChan chan bool

func msgLT(i int, j int) bool {
	return messageQueue[i].msg.Clock < messageQueue[j].msg.Clock
}

// Message ...
type Message struct {
	Command string
	Value   string
	Clock   int
}

// Tuple ...
type Tuple struct {
	msg        Message
	ackCounter int
	msgID      int
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
		handleMessage(message, localHost, host, otherHosts)
		// RELEASE MUTEX HERE
		messageMutex.Unlock()
	}
}

func searchMessageQueue(id int) int {
	for i, tup := range messageQueue {
		if tup.msgID == id {
			return i
		}
	}
	return -1
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
		fmt.Printf("ID %d - CLOCK VALUE BEFORE: %v\n", localHost.ID, logicClock)
		// Push message to queue (id counter minus extra minus self)
		messageQueue = append(messageQueue, Tuple{msg, len(otherHosts), msgIDCounter})
		// Sort queue
		sort.Slice(messageQueue, msgLT)
		timestamp := msg.Clock
		if logicClock < timestamp {
			logicClock = timestamp
		}
		logicClock++ // Is this necessary?
		fmt.Printf("ID %d - CLOCK VALUE AFTER: %v\n", localHost.ID, logicClock)
		// send ack messages to everyone (?)
		acknowledgement := Message{"ACK", strconv.Itoa(msgIDCounter), logicClock}
		msgIDCounter++
		for i := range otherHosts {
			fmt.Printf("ID %d - Sending message to %v:\n%v\n", localHost.ID, otherHosts[i], acknowledgement)
			sendMessage(otherHosts[i].ListenSocket, acknowledgement)
		}
	case "ACK":
		// echo acknowledgement with same id
		response := Message{"ACK_CONF", msg.Value, logicClock}
		fmt.Printf("ID %d - Sending message to %v:\n%v\n", localHost.ID, host, response)
		sendMessage(host.ListenSocket, response)
	case "ACK_CONF":
		id, err := strconv.Atoi(msg.Value)
		check(err)
		tupIndex := searchMessageQueue(id)
		fmt.Println(id)
		messageQueue[tupIndex].ackCounter--
		if messageQueue[tupIndex].ackCounter == 0 && tupIndex == 0 {
			currentTuple := messageQueue[0]
			for currentTuple.ackCounter == 0 {
				fmt.Printf("ID %d - Executing message with ID: %d\n", localHost.ID, currentTuple.msgID)
				messageQueue = messageQueue[1:]
				if len(messageQueue) > 0 {
					currentTuple = messageQueue[0]
				} else {
					break
				}
			}
		}
		if len(messageQueue) == 0 {
			queueSignalChan <- true
		}
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
			msg := Message{"ADDRESS", fmt.Sprint(localHost), logicClock}
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
		fmt.Printf("ID %d - CLOCK VALUE BEFORE: %v\n", localHost.ID, logicClock)
		logicClock++
		target := comm.FindHostByID(msgIns.TargetID, otherHosts)
		msg := Message{"MSG", "MSG", logicClock}
		fmt.Printf("ID %d - Sending message to %v:\n%v\n", localHost.ID, target, msg)
		sendMessage(target.ListenSocket, msg)
		fmt.Printf("ID %d - CLOCK VALUE AFTER: %v\n", localHost.ID, logicClock)
	} else if okMul {
		if mulIns.SenderID != localHost.ID {
			fmt.Printf("ID %d - Skipping instruction %v...\n", localHost.ID, mulIns)
			return
		}
		fmt.Printf("ID %d - RUNNING INSTRUCTION: %v\n", localHost.ID, mulIns)
		fmt.Printf("ID %d - CLOCK VALUE BEFORE: %v\n", localHost.ID, logicClock)
		logicClock++
		for _, targetID := range mulIns.TargetIDs {
			target := comm.FindHostByID(targetID, otherHosts)
			msg := Message{"MSG", "MSG", logicClock}
			fmt.Printf("ID %d - Sending message to %v:\n%v\n", localHost.ID, target, msg)
			sendMessage(target.ListenSocket, msg)
		}
		fmt.Printf("ID %d - CLOCK VALUE AFTER: %v\n", localHost.ID, logicClock)
	} else if okInc {
		if incIns.ID != localHost.ID {
			fmt.Printf("ID %d - Skipping instruction %v...\n", localHost.ID, incIns)
			return
		}
		fmt.Printf("ID %d - RUNNING INSTRUCTION: %v\n", localHost.ID, incIns)
		fmt.Printf("ID %d - CLOCK VALUE BEFORE: %v\n", localHost.ID, logicClock)
		fmt.Printf("ID %d - Incrementing clock by %v...\n", localHost.ID, incIns.Increment)
		logicClock += incIns.Increment
		fmt.Printf("ID %d - CLOCK VALUE AFTER: %v\n", localHost.ID, logicClock)
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

	// Create signal channel
	queueSignalChan = make(chan bool, 100)
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

	// Done reading instructions and establishing connections
	// Send START message
	for _, host := range otherHosts {
		msg := Message{"START", "START", logicClock}
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

	fmt.Printf("ID %d - Instructions done. Waiting for other processes...\n", localHost.ID)

	for len(messageQueue) != 0 {
		fmt.Println(messageQueue)
		// <-queueSignalChan
	}
	// Send END message
	for _, host := range otherHosts {
		if host.IsActive {
			msg := Message{"END", "END", logicClock}
			fmt.Printf("ID %d - Sending message to %v: \n %v \n", localHost.ID, host, msg)
			sendMessage(host.ListenSocket, msg)
		}
	}

	wgEnd.Wait()
	fmt.Printf("ID %d - All processes done. Terminating program...\n", localHost.ID)
	// Close all sockets
	localHost.ListenSocket.Close()
	for _, host := range otherHosts {
		host.ListenSocket.Close()
		host.ClientSocket.Close()
	}
	fmt.Printf("ID %d - FINAL CLOCK: %v\n", localHost.ID, logicClock)
}
