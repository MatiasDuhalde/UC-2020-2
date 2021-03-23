package covid

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"
)

const csvPath = "Covid-19_std.csv"

// LayoutISO defines the format used for dates
const LayoutISO = "2006-01-02"

// CasesData contains a row of information retrieved from Covid-19_std.csv file
type CasesData struct {
	ValidFields    [7]bool
	Region         string
	RegionCode     int
	Comuna         string
	ComunaCode     int
	Population     float64
	Date           time.Time
	ConfirmedCases float64
}

// Fields describes the fields (in order) of CasesData struct
var Fields = [7]string{"Region", "Codigo region", "Comuna", "Codigo comuna", "Poblacion", "Fecha", "Casos confirmados"}

// InvalidDataError is an Error object that may be raised during rowToObj
type InvalidDataError struct {
	msg string
}

func (e *InvalidDataError) Error() string {
	return e.msg
}

// GetValue returns the correct attribute based on the name of the column provided
func (data CasesData) GetValue(colName string) interface{} {
	switch colName {
	case "Region":
		return data.Region
	case "Codigo region":
		return data.RegionCode
	case "Comuna":
		return data.Comuna
	case "Codigo comuna":
		return data.ComunaCode
	case "Poblacion":
		return data.Population
	case "Fecha":
		return data.Date
	case "Casos confirmados":
		return data.ConfirmedCases
	}
	return &InvalidDataError{fmt.Sprintf("Invalid column name '%v'", colName)}
}

// SetValue changes the value of the given attribute
// Checks types before assigning
// Returns an error if a value is invalid
func (data *CasesData) SetValue(colName string, newValue interface{}) error {
	switch colName {
	case "Region":
		if newValue == nil {
			data.ValidFields[0] = false
			break
		}
		v, ok := newValue.(string)
		if !ok {
			msg := fmt.Sprintf("Invalid value %v for column '%v'", newValue, colName)
			return &InvalidDataError{msg}
		}
		data.Region = v
	case "Codigo region":
		if newValue == nil {
			data.ValidFields[1] = false
			break
		}
		v, ok := newValue.(int)
		if !ok {
			msg := fmt.Sprintf("Invalid value %v for column '%v'", newValue, colName)
			return &InvalidDataError{msg}
		}
		data.RegionCode = v
	case "Comuna":
		if newValue == nil {
			data.ValidFields[2] = false
			break
		}
		v, ok := newValue.(string)
		if !ok {
			msg := fmt.Sprintf("Invalid value %v for column '%v'", newValue, colName)
			return &InvalidDataError{msg}
		}
		data.Comuna = v
	case "Codigo comuna":
		if newValue == nil {
			data.ValidFields[3] = false
			break
		}
		v, ok := newValue.(int)
		if !ok {
			msg := fmt.Sprintf("Invalid value %v for column '%v'", newValue, colName)
			return &InvalidDataError{msg}
		}
		data.ComunaCode = v
	case "Poblacion":
		if newValue == nil {
			data.ValidFields[4] = false
			break
		}
		v, ok := newValue.(float64)
		if !ok {
			msg := fmt.Sprintf("Invalid value %v for column '%v'", newValue, colName)
			return &InvalidDataError{msg}
		}
		data.Population = v
	case "Fecha":
		if newValue == nil {
			data.ValidFields[5] = false
			break
		}
		v, ok := newValue.(time.Time)
		if !ok {
			msg := fmt.Sprintf("Invalid value %v for column '%v'", newValue, colName)
			return &InvalidDataError{msg}
		}
		data.Date = v
	case "Casos confirmados":
		if newValue == nil {
			data.ValidFields[6] = false
			break
		}
		v, ok := newValue.(float64)
		if !ok {
			msg := fmt.Sprintf("Invalid value %v for column '%v'", newValue, colName)
			return &InvalidDataError{msg}
		}
		data.ConfirmedCases = v
	default:
		return &InvalidDataError{fmt.Sprintf("Invalid column name '%v'", colName)}
	}
	return nil
}

// ToString returns a string representation of the struct
// Meant to be used with hashing function
func (data *CasesData) ToString() string {
	result := "/"
	for i, isValid := range data.ValidFields {
		if isValid {
			result += fmt.Sprintf("%v/", data.GetValue(Fields[i]))
		}
	}
	return result
}

// ResetValidity sets all booleans of ValidFields to true
func (data *CasesData) ResetValidity() {
	for i := range data.ValidFields {
		data.ValidFields[i] = true
	}
}

// RowToObj converts data in a row (directly from file) to a CasesData object
func RowToObj(data []string) (CasesData, error) {
	var newObj CasesData
	// Region
	newObj.ValidFields[0] = true
	newObj.Region = data[0]
	// RegionCode
	n1, err := strconv.Atoi(data[1])
	if err != nil {
		msg := "Non numeric value for 'Codigo region'"
		return newObj, &InvalidDataError{msg}
	}
	newObj.ValidFields[1] = true
	newObj.RegionCode = n1
	// Comuna
	newObj.ValidFields[2] = true
	newObj.Comuna = data[2]
	// ComunaCode
	n2, err := strconv.Atoi(data[3])
	if err != nil {
		msg := "Non numeric value for 'Codigo comuna'"
		return newObj, &InvalidDataError{msg}
	}
	newObj.ValidFields[3] = true
	newObj.ComunaCode = n2
	// Poblacion
	n3, err := strconv.ParseFloat(data[4], 64)
	if err != nil {
		msg := "Non numeric value for 'Poblacion'"
		return newObj, &InvalidDataError{msg}
	}
	newObj.ValidFields[4] = true
	newObj.Population = n3
	// Fecha
	date, err := time.Parse(LayoutISO, data[5])
	if err != nil {
		msg := "Invalid date format for 'Fecha'"
		return newObj, &InvalidDataError{msg}
	}
	newObj.ValidFields[5] = true
	newObj.Date = date
	// Casos confirmados
	n4, err := strconv.ParseFloat(data[6], 64)
	if err != nil {
		msg := "Non numeric value for 'Casos confirmados'"
		return newObj, &InvalidDataError{msg}
	}
	newObj.ValidFields[6] = true
	newObj.ConfirmedCases = n4
	return newObj, nil
}

// OpenFile opens covid data file
// returns an array of arrays of strings
func OpenFile() [][]string {
	f, err := os.Open(csvPath)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer f.Close()

	lines, err := csv.NewReader(f).ReadAll()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return lines
}

// ParseLines receives the output of OpenFile and turns every row into an
// equivalent CasesData object
func ParseLines(input [][]string) []*CasesData {
	objectArray := make([]*CasesData, len(input))
	// If input contains headers, RowToObj should automatically skip it
	i := 0
	for lineNumber, row := range input {
		result, err := RowToObj(row)
		if err == nil {
			objectArray[i] = &result
			i++
		} else {
			fmt.Printf("Line %v: %v\n", lineNumber+2, err)
		}
	}
	return objectArray[:i]
}
