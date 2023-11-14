package godb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
)

type EmbeddingType []float64

type EmbeddingResponse struct {
	Embedding EmbeddingType `json:"embedding"`
}

var portNumber string = "7010"

func generateEmbeddings(text string) (*EmbeddingResponse, error) {

	//Format text to string
	data := map[string]interface{}{
		"text": text,
	}

	// Convert data to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		fmt.Println("Error marshaling JSON:", err)
		return nil, err
	}

	// Send a POST request to the Python server
	resp, err := http.Post("http://localhost:"+portNumber+"/embed", "application/json",
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
	var embeddingResp EmbeddingResponse
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&embeddingResp); err != nil {
		fmt.Println("Error decoding response JSON:", err)
		return nil, err
	}

	return &embeddingResp, nil
}

type DimResponse struct {
	Dimension int `json:"dimemb"`
}

func getEmbeddingDim() int {
	// Format text to string
	data := map[string]interface{}{}

	// Convert data to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		panic(err.Error())
	}

	// Send a POST request to the Python server
	resp, err := http.Post("http://localhost:"+portNumber+"/dimemb", "application/json",
		bytes.NewBuffer(jsonData))
	if err != nil {
		fmt.Println("Error sending HTTP request:", err)
		panic(err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Println("Received non-OK status code:", resp.Status)
		panic(err.Error())
	}

	// Decode the response JSON
	var dimResp DimResponse
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&dimResp); err != nil {
		err.Error()
	}

	return dimResp.Dimension
}

func dotProduct(v1, v2 *EmbeddingType) (float64, error) {
	if len(*v1) != len(*v2) {
		return 0.0, fmt.Errorf("Length mismatch: %d vs %d", len(*v1), len(*v2))
	}
	var dotprod float64 = 0.0
	for i := 0; i < len(*v1); i++ {
		dotprod += (*v1)[i] * (*v2)[i]
	}
	return dotprod, nil
}

func CosDist(v1, v2 *EmbeddingType) (float64, error) {

	if len(*v1) != len(*v2) {
		return 0.0, fmt.Errorf("Length mismatch: %d vs %d", len(*v1), len(*v2))
	}
	var dotprod float64 = 0.0
	var squaredSumV1 float64 = 0.0
	var squaredSumV2 float64 = 0.0

	for i := 0; i < len(*v1); i++ {
		dotprod += (*v1)[i] * (*v2)[i]
		squaredSumV1 += (*v1)[i] * (*v1)[i]
		squaredSumV2 += (*v2)[i] * (*v2)[i]
	}
	normV1 := math.Sqrt(squaredSumV1)
	normV2 := math.Sqrt(squaredSumV2)

	cosdist := dotprod / (normV1 * normV2)

	return cosdist, nil
}
