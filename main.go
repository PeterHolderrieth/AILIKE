package main

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

func main() {
	dataTexts := []string{"Twitter, Inc. was an American social media company based in San Francisco, California. The company operated the social networking service Twitter and previously the Vine short video app and Periscope livestreaming service. In April 2023, Twitter merged with X Holdings and ceased to be an independent company, becoming a part of X Corp.",
		"Space Exploration Technologies Corp., commonly referred to as SpaceX, is an American spacecraft manufacturer, launch service provider, defense contractor and satellite communications company headquartered in Hawthorne, California. The company was founded in 2002 by Elon Musk with the goal of reducing space transportation costs and to colonize Mars. The company currently operates the Falcon 9 and Falcon Heavy rockets along with the Dragon spacecraft.",
		"The ferns (Polypodiopsida or Polypodiophyta) are a group of vascular plants (plants with xylem and phloem) that reproduce via spores and have neither seeds nor flowers. They differ from mosses by being vascular, i.e., having specialized tissues that conduct water and nutrients and in having life cycles in which the branched sporophyte is the dominant phase.",
		"A leaf (pl: leaves) is a principal appendage of the stem of a vascular plant,[1] usually borne laterally aboveground and specialized for photosynthesis. Leaves are collectively called foliage, as in autumn foliage,[2][3] while the leaves, stem, flower, and fruit collectively form the shoot system.[4] In most leaves, the primary photosynthetic tissue is the palisade mesophyll."}

	queryText := "I search for a plant that grows from soil."
	queryEmbedding, err := generateEmbeddings(queryText)
	fmt.Println("Query text: ", queryText)
	fmt.Println("")
	if err != nil {
		return
	}

	for _, dataText := range dataTexts {
		embedding, err := generateEmbeddings(dataText)
		if err != nil {
			fmt.Println("Generate embeddings resulted in error")
			return
		}
		dist, err := CosDist(&queryEmbedding.Embedding, &embedding.Embedding)
		if err != nil {
			fmt.Println("Cosine dist resulted in error: ", err.Error())
			return
		}
		fmt.Println("Similarity: ", dist, " ", dataText)
		fmt.Println("")

	}
}
