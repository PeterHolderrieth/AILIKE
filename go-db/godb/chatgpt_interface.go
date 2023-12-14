package godb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

var portNumberChatGPT string = "7012"

type chatGPTResponse struct {
	Response string `json:"Response"`
}

func GenerateChatGPTFactCheck(fact string, query_matches []int64, withContext bool) (string, error) {

	//Format text to string
	data := map[string]interface{}{
		"fact":          fact,
		"query_matches": query_matches,
		"with_context":  withContext,
	}

	// Convert data to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		fmt.Println("Error marshaling JSON:", err)
		return "", err
	}

	// Send a POST request to the Python server
	resp, err := http.Post("http://localhost:"+portNumberChatGPT+"/chatgptfactcheck", "application/json",
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
	var chatGPTResp chatGPTResponse
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&chatGPTResp); err != nil {
		fmt.Println("Error decoding response JSON:", err)
		return "", err
	}

	return chatGPTResp.Response, nil
}
