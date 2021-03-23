package ins

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// MulticastInstruction ...
type MulticastInstruction struct {
	SenderID  int
	TargetIDs []int
}

func (insObj MulticastInstruction) String() string {
	return fmt.Sprintf("C%v M C%v", insObj.SenderID, strings.ReplaceAll(strings.Trim(fmt.Sprint(insObj.TargetIDs), "[]"), " ", " C"))
}

// MessageInstruction ...
type MessageInstruction struct {
	SenderID int
	TargetID int
}

func (insObj MessageInstruction) String() string {
	return fmt.Sprintf("C%v M C%v", insObj.SenderID, insObj.TargetID)
}

// IncrementInstruction ...
type IncrementInstruction struct {
	ID        int
	Increment int
}

func (insObj IncrementInstruction) String() string {
	return fmt.Sprintf("C%v A %v", insObj.ID, insObj.Increment)
}

// Instruction ...
type Instruction interface {
}

// ParseID ...
func ParseID(s string, maxID int) (int, error) {
	id, err := strconv.Atoi(s[1:])
	if err != nil {
		return id, err
	}
	if id > maxID || id < 0 {
		return id, fmt.Errorf("Error")
	}
	return id, nil
}

// ParseInstruction ...
func ParseInstruction(s string, maxID int) (Instruction, error) {
	insSlice := strings.Split(s, " ")
	if len(insSlice) < 3 {
		return MessageInstruction{}, fmt.Errorf("Invalid instruction %s", s)
	}
	id, err := ParseID(insSlice[0], maxID)
	if err != nil {
		return MessageInstruction{}, fmt.Errorf("Invalid ID in instruction %s", s)
	}

	switch insSlice[1] {
	case "M":
		if len(insSlice[2:]) > 1 {
			ids := make([]int, len(insSlice[2:]))
			for i := range insSlice[2:] {
				targetID, err := ParseID(insSlice[i+2], maxID)
				if err != nil {
					return MessageInstruction{}, fmt.Errorf("Invalid ID in instruction %s", s)
				}
				ids[i] = targetID
			}
			insObj := MulticastInstruction{id, ids}
			return insObj, nil
		}
		targetID, err := ParseID(insSlice[2], maxID)
		if err != nil {
			return MessageInstruction{}, fmt.Errorf("Invalid ID in instruction %s", s)
		}
		insObj := MessageInstruction{id, targetID}
		return insObj, nil

	case "A":
		inc, err := strconv.Atoi(insSlice[2])
		if err != nil {
			return MessageInstruction{}, fmt.Errorf("Invalid value in instruction %s", s)
		}
		insObj := IncrementInstruction{id, inc}
		return insObj, nil
	}
	return MessageInstruction{}, fmt.Errorf("Invalid instruction %s", s)
}

func check(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// ReadInstructions ...
func ReadInstructions(path string, maxID int) []Instruction {
	f, err := os.Open(path)
	check(err)
	defer f.Close()

	var result []Instruction
	r := bufio.NewReader(f)

	err = nil
	var buffer string
	for err != io.EOF {
		check(err)
		buffer, err = r.ReadString('\n')
		buffer = strings.Trim(buffer, "\r\n")
		insObj, err := ParseInstruction(buffer, maxID)
		check(err)
		result = append(result, insObj)
	}

	return result
}
