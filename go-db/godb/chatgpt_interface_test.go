package godb

import (
	"fmt"
	"testing"
)

func TestGenerateChatGPTFactCheck(t *testing.T) {

	fact := "Aristotle is from Greece."

	query_matches := []int64{0, 1}

	result, err := generateChatGPTFactCheck(fact, query_matches)
	if err != nil {
		t.Fatalf(err.Error())
	}

	fmt.Println("Result: ", result)
}
