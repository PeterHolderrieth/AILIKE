package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/srmadden/godb"
)

type GenResponse struct {
	Response string `json:"response"`
}

func getLLMResponse(text string) (string, error) {

	//Format text to string
	data := map[string]interface{}{
		"text": text,
	}

	// Convert data to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		fmt.Println("Error marshaling JSON:", err)
		return "", err
	}

	portNumberLLM := "7020"
	// Send a POST request to the Python server
	resp, err := http.Post("http://localhost:"+portNumberLLM+"/gen", "application/json",
		bytes.NewBuffer(jsonData))
	if err != nil {
		fmt.Println("Error sending HTTP request:", err)
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Println("Received non-OK status code:", resp.Status)
		return "", err
	}

	// Decode the response JSON
	var genResponse GenResponse
	decoder := json.NewDecoder(resp.Body)

	if err := decoder.Decode(&genResponse); err != nil {
		fmt.Println("Error decoding response JSON:", err)
		return "", err
	}

	return genResponse.Response, nil
}

type QueryMetaData struct {
	catalog string
	path    string
	bpSize  int
}

func RunQuery(query string, config QueryMetaData)  (error) {
	bp := godb.NewBufferPool(config.bpSize)
	c, err := godb.NewCatalogFromFile(config.catalog, bp, config.path)
	if err != nil {
		return err
	}
	qType, plan, err := godb.Parse(c, query)
	if err != nil {
		return err
	}
	if plan == nil {
		return err
	}
	if qType != godb.IteratorType {
		return err
	}
	desc := plan.Descriptor()
	if desc == nil {
		return err
	}
	tid := godb.NewTID()
	iter, err := plan.Iterator(tid)
	if err != nil {
		return err
	}

	for {
		tup, err := iter()
		fmt.Println("TEST")
		if err != nil {
			fmt.Printf("%s\n", err.Error())
			break
		}
		if tup == nil {
			break
		} else {
			fmt.Printf("\033[32m%s\033[0m\n", tup.PrettyPrintString(true))
		}
		}

	bp.CommitTransaction(tid)
	return nil
}

func main() {
	for {
		fmt.Print("> ")
		line := ""
		in    := bufio.NewReader(os.Stdin)
		line, err := in.ReadString('\n')
    fmt.Println(line) // or do something else with line
		if err != nil{
			fmt.Println("error reading input")
			break;
		}
		if line == "exit" {
			break
	 }

		resp, err := getLLMResponse("System:Return 5 similar words to the query\nUser:" + line + "\nAssistant:")
		if err != nil {
			log.Printf("Could not generate LLM embedding\n%s", err)
		}

		fmt.Println(resp)
		query := "select tweet_id, sentiment, content, ('"+ resp +"' ailike content) sim from tweets_mini order by sim limit 5"
		// query := "select tweet_id, sentiment, content, ('test' ailike content) sim from tweets_mini order by sim limit 5"
		fmt.Println(query)
		var config = QueryMetaData{
			catalog: "tweets_384.catalog",
			path:    "../data/tweets/tweets_384/",
			bpSize:  10}
		err = RunQuery(query, config)
		if err != nil{
			fmt.Println(err)
		}
	}
}
