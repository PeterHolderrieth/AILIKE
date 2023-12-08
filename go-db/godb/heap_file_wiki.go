package godb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
)

type DataElement map[string]string

type WikiResponse struct {
	Datael DataElement `json:"dataelement"`
}

var portNumberWiki int = 7011

func ConstructWikiHeapFile(tableName string, bp *BufferPool, resetFile bool, limit int) (*HeapFile, error) {

	td := &TupleDesc{Fields: []FieldType{
		{Fname: "idx_data", Ftype: IntType},
		{Fname: "id", Ftype: IntType},
		{Fname: "url", Ftype: StringType},
		{Fname: "title", Ftype: StringType},
		{Fname: "articleStart", Ftype: EmbeddedStringType},
	}}

	fileName := tableName + ".dat"
	if resetFile {
		os.Remove(fileName)
		hf, err := NewHeapFile(fileName, td, bp)
		if err != nil {
			return nil, err
		}
		fmt.Println("Load from API")
		err = hf.LoadFromAPI(portNumberWiki, limit)
		if err != nil {
			return nil, err
		}
		return hf, nil

	} else {
		hf, err := NewHeapFile(fileName, td, bp)
		if err != nil {
			return nil, err
		}
		return hf, nil
	}
}

func (f *HeapFile) LoadFromAPI(portNumber int, limit int) error {

	desc := f.Descriptor()
	if desc == nil || desc.Fields == nil {
		return GoDBError{MalformedDataError, "Descriptor was nil"}
	}
	var counter int = 0
	var wikiresponse *WikiResponse
	var err error
	for counter < limit {
		wikiresponse, err = getWikiElement(counter)

		if err != nil {
			return err
		}
		var newFields []DBValue
		for _, field := range desc.Fields {

			fieldValue := wikiresponse.Datael[field.Fname]

			switch field.Ftype {
			case IntType:
				fieldValue = strings.TrimSpace(fieldValue)
				floatVal, err := strconv.ParseFloat(fieldValue, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromAPI: couldn't convert value %s to int, tuple %d", fieldValue, counter)}
				}
				intValue := int(floatVal)
				newFields = append(newFields, IntField{int64(intValue)})

			case StringType:
				if len(fieldValue) > StringLength {
					fieldValue = fieldValue[0:StringLength]
				}
				newFields = append(newFields, StringField{fieldValue})

			case EmbeddedStringType:
				if len(fieldValue) > TextCharLength {
					fieldValue = fieldValue[0:TextCharLength]
				}
				newFields = append(newFields, EmbeddedStringField{Value: fieldValue})

			default:
				return GoDBError{code: IncompatibleTypesError, errString: "(LoadFromHeapFile): Unknown type."}
			}
		}
		counter++
		newT := Tuple{*desc, newFields, nil}
		tid := NewTID()
		bp := f.bufPool
		bp.BeginTransaction(tid)
		f.insertTuple(&newT, tid)
		bp.CommitTransaction(tid)
		if (counter % 100) == 0 {
			fmt.Println("Inserted tuple: ", counter)
		}
	}
	return nil
}

func getWikiElement(idx_query int) (*WikiResponse, error) {

	//Format text to string
	data := map[string]interface{}{
		"idx":         idx_query,
		"char_length": TextCharLength,
	}

	// Convert data to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		fmt.Println("Error marshaling JSON:", err)
		return nil, err
	}

	// Send a POST request to the Python server
	resp, err := http.Post("http://localhost:"+fmt.Sprint(portNumberWiki)+"/dataitem", "application/json",
		bytes.NewBuffer(jsonData))
	if err != nil {
		fmt.Println("Error sending HTTP request:", err)
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Println("Received non-OK status code:", resp.Status)
		return nil, err
	}
	// Decode the response JSON
	var wikiresp WikiResponse
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&wikiresp); err != nil {
		fmt.Println("Error decoding response JSON:", err)
		return nil, err
	}
	wikiresp.Datael["idx_data"] = fmt.Sprint(idx_query)
	return &wikiresp, nil
}
