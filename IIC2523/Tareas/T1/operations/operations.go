package operations

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const outputPath = "out.csv"

// Filters map a string representing a boolean comparison to its equivalent function
var Filters = map[string]func(interface{}, interface{}) bool{
	"<":  lt,
	"<=": le,
	"==": eq,
	"!=": ne,
	">=": ge,
	">":  gt,
}

// ValidFunctions contains the strings of functions allowed
var ValidFunctions = []string{"MIN", "MAX", "AVG", "SUM"}

// stringToData receives a string and returns an integer, float64, date, or string
// in that order of priority
func stringToData(str string) interface{} {
	// int
	n, err := strconv.Atoi(str)
	if err == nil {
		return n
	}
	f, err := strconv.ParseFloat(str, 64)
	if err == nil {
		return f
	}
	date, err := time.Parse("2006-01-02", str)
	if err == nil {
		return date
	}
	return str
}

func interfaceToNumeric(a interface{}, b interface{}) (float64, float64, bool) {
	var valueA, valueB float64
	ok := true
	switch reflect.TypeOf(a).Kind() {
	case reflect.Float64:
		valueA = a.(float64)
	case reflect.Int:
		valueA = float64(a.(int))
	default:
		ok = false
	}
	switch reflect.TypeOf(b).Kind() {
	case reflect.Float64:
		valueB = b.(float64)
	case reflect.Int:
		valueB = float64(b.(int))
	default:
		ok = false
	}
	return valueA, valueB, ok
}

func lt(a interface{}, b interface{}) bool {
	aString, aIsString := a.(string)
	bString, bIsString := b.(string)
	if aIsString && bIsString {
		res := strings.Compare(aString, bString)
		return res < 0
	}
	aDate, aIsDate := a.(time.Time)
	bDate, bIsDate := b.(time.Time)
	if aIsDate && bIsDate {
		return aDate.Before(bDate)
	}
	aNumeric, bNumeric, ok := interfaceToNumeric(a, b)
	if ok {
		return aNumeric < bNumeric
	}
	return false
}

func le(a interface{}, b interface{}) bool {
	aString, aIsString := a.(string)
	bString, bIsString := b.(string)
	if aIsString && bIsString {
		res := strings.Compare(aString, bString)
		return res <= 0
	}
	aDate, aIsDate := a.(time.Time)
	bDate, bIsDate := b.(time.Time)
	if aIsDate && bIsDate {
		return aDate.Before(bDate) || aDate == bDate
	}
	aNumeric, bNumeric, ok := interfaceToNumeric(a, b)
	if ok {
		return aNumeric <= bNumeric
	}
	return false
}

func eq(a interface{}, b interface{}) bool {
	aString, aIsString := a.(string)
	bString, bIsString := b.(string)
	if aIsString && bIsString {
		res := strings.Compare(aString, bString)
		return res == 0
	}
	aDate, aIsDate := a.(time.Time)
	bDate, bIsDate := b.(time.Time)
	if aIsDate && bIsDate {
		return aDate == bDate
	}
	aNumeric, bNumeric, ok := interfaceToNumeric(a, b)
	if ok {
		return aNumeric == bNumeric
	}
	return false
}

func ne(a interface{}, b interface{}) bool {
	aString, aIsString := a.(string)
	bString, bIsString := b.(string)
	if aIsString && bIsString {
		res := strings.Compare(aString, bString)
		return res != 0
	}
	aDate, aIsDate := a.(time.Time)
	bDate, bIsDate := b.(time.Time)
	if aIsDate && bIsDate {
		return aDate != bDate
	}
	aNumeric, bNumeric, ok := interfaceToNumeric(a, b)
	if ok {
		return aNumeric != bNumeric
	}
	return false
}

func ge(a interface{}, b interface{}) bool {
	aString, aIsString := a.(string)
	bString, bIsString := b.(string)
	if aIsString && bIsString {
		res := strings.Compare(aString, bString)
		return res >= 0
	}
	aDate, aIsDate := a.(time.Time)
	bDate, bIsDate := b.(time.Time)
	if aIsDate && bIsDate {
		return aDate.After(bDate) || aDate == bDate
	}
	aNumeric, bNumeric, ok := interfaceToNumeric(a, b)
	if ok {
		return aNumeric >= bNumeric
	}
	return false
}

func gt(a interface{}, b interface{}) bool {
	aString, aIsString := a.(string)
	bString, bIsString := b.(string)
	if aIsString && bIsString {
		res := strings.Compare(aString, bString)
		return res > 0
	}
	aDate, aIsDate := a.(time.Time)
	bDate, bIsDate := b.(time.Time)
	if aIsDate && bIsDate {
		return aDate.After(bDate)
	}
	aNumeric, bNumeric, ok := interfaceToNumeric(a, b)
	if ok {
		return aNumeric > bNumeric
	}
	return false
}

func getHash(s string) string {
	h := sha1.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

// Operation is the base operation interface for types Selection, Projection, and Group
type Operation interface {
	Mapper([]DataStruct, chan Tuple)
	Reducer(chan Tuple, chan Tuple)
}

// Tuple has a key and a value attribute. Output of Mapper function
type Tuple struct {
	Key   string
	Value interface{}
}

// DataStruct defines a type that has values that can be retrieved through GetValue
type DataStruct interface {
	GetValue(string) interface{}
	SetValue(string, interface{}) error
	ToString() string
	ResetValidity()
}

// Select ...
type Select struct {
	Column string
	Filter func(interface{}, interface{}) bool
	Value  interface{} // string, number (int/float64) or date
}

// Mapper is selects mapper function
func (opObj Select) Mapper(input []DataStruct, output chan Tuple) {
	for _, value := range input {
		if opObj.Filter(value.GetValue(opObj.Column), opObj.Value) {
			// Get key
			key := getHash(value.ToString())
			output <- Tuple{key, value}
		}
	}
}

// Reducer ...
func (opObj Select) Reducer(input chan Tuple, output chan Tuple) {
	for tuple := range input {
		output <- tuple
	}
}

// Projection ...
type Projection struct {
	Columns []string
	Headers []string
}

// Mapper ...
func (opObj Projection) Mapper(input []DataStruct, output chan Tuple) {
	for _, value := range input {
		for _, header := range opObj.Headers {
			if !StringInSlice(header, opObj.Columns) {
				err := value.SetValue(header, nil)
				if err != nil {
					fmt.Println(err)
				}
			}
		}
		key := getHash(value.ToString())
		output <- Tuple{key, value}
	}
}

// Reducer ...
func (opObj Projection) Reducer(input chan Tuple, output chan Tuple) {
	outputted := false
	for tuple := range input {
		if !outputted {
			output <- tuple
			outputted = true
		}
	}
}

// GroupAggregate ...
type GroupAggregate struct {
	Column1  string
	Column2  string
	Function string
}

// Mapper ...
func (opObj GroupAggregate) Mapper(input []DataStruct, output chan Tuple) {
	for _, value := range input {
		key := fmt.Sprintf("%v", value.GetValue(opObj.Column1))
		value := value.GetValue(opObj.Column2)
		output <- Tuple{key, value}
	}

}

// Reducer ...
func (opObj GroupAggregate) Reducer(input chan Tuple, output chan Tuple) {
	switch opObj.Function {
	case "MIN":
		var result interface{}
		result = nil
		var key string
		for tuple := range input {
			key = tuple.Key
			if result == nil {
				result = tuple.Value
			} else {
				if lt(tuple.Value, result) {
					result = tuple.Value
				}
			}
		}
		output <- Tuple{key, result}
	case "MAX":
		var result interface{}
		result = nil
		var key string
		for tuple := range input {
			key = tuple.Key
			if result == nil {
				result = tuple.Value
			} else {
				if gt(tuple.Value, result) {
					result = tuple.Value
				}
			}
		}
		output <- Tuple{key, result}
	case "AVG":
		var result float64
		count := 0.0
		var key string
		for tuple := range input {
			count++
			key = tuple.Key
			result += tuple.Value.(float64)
		}
		output <- Tuple{key, result / count}
	case "SUM":
		var result float64
		var key string
		for tuple := range input {
			key = tuple.Key
			result += tuple.Value.(float64)
		}
		output <- Tuple{key, result}
	}
}

// OperationObject contains relevant information for the execution of an operation
type OperationObject struct {
	SlicesArray     [][][]string
	NumberOfThreads int
	Query           []string
}

// InvalidOperationError is an Error object that may be raised during ParseOperationInput
type InvalidOperationError struct {
	msg string
}

func (e *InvalidOperationError) Error() string {
	return e.msg
}

func getInputPrompt(prompt string) string {
	fmt.Printf("%v: ", prompt)
	return getInput()
}

func getInput() string {
	reader := bufio.NewReader(os.Stdin)
	input, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println(err)
	}
	return input
}

// StringInSlice checks if a string is contained within an array of strings
func StringInSlice(str string, slice []string) bool {
	for _, value := range slice {
		if str == value {
			return true
		}
	}
	return false
}

// GetOperationInput receives an operation input from the user
func GetOperationInput() []string {
	fmt.Println("Enter the operation (insert a blank line to stop reading):")
	var input string
	var operation []string
	for {
		input = getInput()
		// Remove newline according to platform
		// https://stackoverflow.com/questions/19847594/how-to-reliably-detect-os-platform-in-go
		if runtime.GOOS == "windows" {
			input = strings.TrimRight(input, "\r\n")
		} else {
			input = strings.TrimRight(input, "\n")
		}
		if input == "" {
			break
		}
		operation = append(operation, input)
	}
	return operation
}

// ParseOperation checks if an array of strings represents a valid operation
// Returns an operation structure. If input is invalid, returns error object
func ParseOperation(operationInput []string, headers []string) (Operation, error) {
	if len(operationInput) < 3 {
		msg := fmt.Sprintf("Too few lines (%v) for a valid operation", len(operationInput))
		return nil, &InvalidOperationError{msg}
	}
	// OP NAME
	switch opName := strings.ToUpper(operationInput[0]); opName {
	case "SELECT":
		operation := &Select{}
		if len(operationInput) != 4 {
			msg := fmt.Sprintf("Too few lines (%v) for SELECT operation", len(operationInput))
			return nil, &InvalidOperationError{msg}
		}
		// COL NAME
		if !StringInSlice(operationInput[1], headers) {
			msg := fmt.Sprintf("Invalid column name %v", operationInput[1])
			return nil, &InvalidOperationError{msg}
		}
		operation.Column = operationInput[1]
		// FILTER
		operation.Filter = Filters[operationInput[2]]
		if operation.Filter == nil {
			msg := fmt.Sprintf("Invalid filter %v", operationInput[2])
			return nil, &InvalidOperationError{msg}
		}
		// VALUE
		operation.Value = stringToData(operationInput[3])
		return operation, nil
	case "PROJECTION":
		operation := &Projection{}
		// HEADERS
		operation.Headers = headers
		// N
		n, err := strconv.Atoi(operationInput[1])
		if err != nil {
			return nil, err
		}
		if len(operationInput) != 2+n {
			msg := fmt.Sprintf("Number of columns (%v) does not match N value %v", len(operationInput)-2, n)
			return nil, &InvalidOperationError{msg}
		}
		// COL NAMES
		for i := 0; i < n; i++ {
			if !StringInSlice(operationInput[2+i], headers) {
				msg := fmt.Sprintf("Invalid column name %v", operationInput[2+i])
				return nil, &InvalidOperationError{msg}
			}
		}
		operation.Columns = operationInput[2:]
		return operation, nil
	case "GROUP":
		operation := &GroupAggregate{}
		if len(operationInput) != 5 {
			msg := fmt.Sprintf("Too few lines (%v) for GROUP operation", len(operationInput))
			return nil, &InvalidOperationError{msg}
		}
		// COL NAME 0
		if !StringInSlice(operationInput[1], headers) {
			msg := fmt.Sprintf("Invalid column name %v", operationInput[1])
			return nil, &InvalidOperationError{msg}
		}
		operation.Column1 = operationInput[1]
		// AGGREGATE
		if strings.ToUpper(strings.ToUpper(operationInput[2])) != "AGGREGATE" {
			msg := "Third line should be 'AGGREGATE'"
			return nil, &InvalidOperationError{msg}
		}
		// COL NAME 1
		if !StringInSlice(operationInput[3], headers) {
			msg := fmt.Sprintf("Invalid column name %v", operationInput[3])
			return nil, &InvalidOperationError{msg}
		}
		operation.Column2 = operationInput[3]
		// FUNCTION
		if !StringInSlice(strings.ToUpper(operationInput[4]), ValidFunctions) {
			msg := fmt.Sprintf("Invalid function %v", operationInput[4])
			return nil, &InvalidOperationError{msg}
		}
		operation.Function = operationInput[4]
		return operation, nil
	}
	msg := fmt.Sprintf("Unsupported operation %v", operationInput[0])
	return nil, &InvalidOperationError{msg}
}

// OutputToFile writes the headers and
func OutputToFile(input chan Tuple, headers []string, opObj Operation) {
	f, _ := os.Create(outputPath)
	w := bufio.NewWriter(f)
	pObj, ok := opObj.(*Projection)
	if ok {
		headers = pObj.Columns
	} else {
		gObj, ok := opObj.(*GroupAggregate)
		if ok {
			headers = []string{gObj.Column1, fmt.Sprintf("%v %v", gObj.Function, gObj.Column2)}
		}
	}

	fmt.Fprintf(w, "%v\n", strings.Join(headers, ","))
	for tuple := range input {
		data, isDataStruct := tuple.Value.(DataStruct)
		values := make([]string, len(headers))
		for i, header := range headers {
			if isDataStruct {
				value := data.GetValue(header)
				if value != nil {
					date, isDate := value.(time.Time)
					if isDate {
						values[i] = fmt.Sprintf("%d-%02d-%02d", date.Year(), date.Month(), date.Day())
					} else {
						values[i] = fmt.Sprintf("%v", value)
					}
				}
			} else {
				values[0] = fmt.Sprintf("%v", tuple.Key)
				values[1] = fmt.Sprintf("%v", tuple.Value)
			}
		}
		fmt.Fprintf(w, "%v\n", strings.Join(values, ","))
		w.Flush()
	}
	f.Close()
}
