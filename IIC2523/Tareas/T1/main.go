package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"./covid"
	"./operations"
)

// SetupCloseHandler activates a handle to catch CTRL + C (SIGTERM)
// https://golangcode.com/handle-ctrl-c-exit-in-terminal/
func SetupCloseHandler() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\r- Ctrl+C pressed in Terminal")
		fmt.Println("\r- Exiting...")
		os.Exit(0)
	}()
}

// PartitionArray splits an array into N slices
func PartitionArray(sourceArray []operations.DataStruct,
	numberOfThreads int) [][]operations.DataStruct {
	step := len(sourceArray) / numberOfThreads
	slicesArray := make([][]operations.DataStruct, numberOfThreads)
	for i := 0; i < numberOfThreads-1; i++ {
		slicesArray[i] = sourceArray[i*step : (i+1)*step]
	}
	slicesArray[numberOfThreads-1] = sourceArray[(numberOfThreads-1)*step:]
	return slicesArray
}

// PartitionTupleArray splits a Tuple array into N slices
func PartitionTupleArray(sourceArray []operations.Tuple,
	numberOfThreads int) [][]operations.Tuple {
	step := len(sourceArray) / numberOfThreads
	// fmt.Println(len(sourceArray), step)
	slicesArray := make([][]operations.Tuple, numberOfThreads)
	for i := 0; i < numberOfThreads-1; i++ {
		slicesArray[i] = sourceArray[i*step : (i+1)*step]
	}
	slicesArray[numberOfThreads-1] = sourceArray[(numberOfThreads-1)*step:]
	return slicesArray
}

// Map mapping phase
func Map(dataArray []operations.DataStruct, numberOfThreads int,
	operation operations.Operation) []operations.Tuple {
	var wgWorkers sync.WaitGroup
	var wgReceiver sync.WaitGroup

	// Split array
	dataSlices := PartitionArray(dataArray, numberOfThreads-1)
	// Allocate space
	output := make([]operations.Tuple, len(dataArray))
	workerOutput := make(chan operations.Tuple)
	// Receiver thread
	go func() {
		wgReceiver.Add(1)
		i := 0
		for out := range workerOutput {
			output[i] = out
			i++
		}
		output = output[:i]
		wgReceiver.Done()
	}()
	// Initialize each thread
	for i := 0; i < numberOfThreads-1; i++ {
		wgWorkers.Add(1)
		go func(arr []operations.DataStruct) {
			operation.Mapper(arr, workerOutput)
			wgWorkers.Done()
		}(dataSlices[i])
	}
	// Wait for all worker threads
	wgWorkers.Wait()
	// Close channel
	close(workerOutput)
	// Wait for receiver thread
	wgReceiver.Wait()

	return output
}

// Shuffle shuffling phase
func Shuffle(dataTuples []operations.Tuple, numberOfThreads int, outputChan chan chan operations.Tuple) {
	// Partition array
	// slicesArray := PartitionTupleArray(dataTuples, numberOfThreads-1)
	chanHashSet := make(map[string]chan operations.Tuple)

	go func() {
		for _, tuple := range dataTuples {
			_, ok := chanHashSet[tuple.Key]
			// If new key create a new channel
			if !ok {
				newChan := make(chan operations.Tuple)
				chanHashSet[tuple.Key] = newChan
				outputChan <- newChan
				// emit tuple through new channel
				newChan <- tuple
			} else {
				// emit tuple through corresponding channel
				chanHashSet[tuple.Key] <- tuple
			}
		}
		// Close all channels
		close(outputChan)
		for _, c := range chanHashSet {
			close(c)
		}
		fmt.Println("All channels closed")
	}()
}

// Reduce reducing phase
func Reduce(inputChan chan chan operations.Tuple, numberOfThreads int,
	operation operations.Operation, output chan operations.Tuple) {

	var wgWorkers sync.WaitGroup
	for recvChan := range inputChan {
		wgWorkers.Add(1)
		go func(c chan operations.Tuple) {
			operation.Reducer(c, output)
			wgWorkers.Done()
		}(recvChan)
	}
	wgWorkers.Wait()
	close(output)
}

func main() {
	// Start CTRL + C Handler
	SetupCloseHandler()

	// Retrieve command-line arguments
	if cap(os.Args) != 2 {
		fmt.Println("Usage: .\\program.go <numberOfThreads>")
		os.Exit(1)
	}
	numberOfThreads, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	} else if numberOfThreads < 2 {
		fmt.Println("Error: numberOfThreads must be an integer greater than 1")
		os.Exit(1)
	}

	// Read data
	lines := covid.OpenFile()
	headers := lines[0]
	casesData := covid.ParseLines(lines[1:])

	// Split data
	dataStructs := make([]operations.DataStruct, len(casesData))
	for i, v := range casesData {
		dataStructs[i] = operations.DataStruct(v)
	}
	for {
		// Get operation input
		input := operations.GetOperationInput()
		operation, err := operations.ParseOperation(input, headers)
		if err != nil {
			fmt.Println(err)
			continue
		}

		mapOutput := Map(dataStructs, numberOfThreads, operation)

		shufflerChan := make(chan chan operations.Tuple)
		reduceOutput := make(chan operations.Tuple)

		// Output
		go operations.OutputToFile(reduceOutput, headers, operation)

		Shuffle(mapOutput, numberOfThreads, shufflerChan)

		Reduce(shufflerChan, numberOfThreads, operation, reduceOutput)

		// Reset valid fields
		for _, data := range dataStructs {
			data.ResetValidity()
		}
	}
}
