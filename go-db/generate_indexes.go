package main

import (
	"fmt"

	"github.com/srmadden/godb"
)

func generate_indexpath(c *godb.Catalog, table string, column string, clusters int, path string) {
	bp := godb.NewBufferPool(1000)
	hf, err := c.GetTable(table)
	fmt.Println("Generating index for ", table)
	_, err = godb.ConstructNNIndexFileFromHeapFile(hf.(*godb.HeapFile), column, clusters, false,
		path, table, bp)

	if err != nil {
		fmt.Println("Failed to construct index file for ", table)
	}
}

func main() {
	catName := "tweets_384.catalog"
	catPath := "../data/tweets/tweets_384/"
	bp := godb.NewBufferPool(1000)
	c, err := godb.NewCatalogFromFile(catName, bp, catPath)
	if err != nil {
		fmt.Printf("failed load catalog, %s", err.Error())
		return
	}
	dbDir := "../data/tweets/tweets_384/"
	generate_indexpath(c, "tweets_c_50", "content", 50, dbDir)
	generate_indexpath(c, "tweets_c_250", "content", 250, dbDir)
	generate_indexpath(c, "tweets_c_100", "content", 100, dbDir)
	generate_indexpath(c, "tweets_c_500", "content", 500, dbDir)
	generate_indexpath(c, "tweets_2500_c_16", "content", 16, dbDir)
	generate_indexpath(c, "tweets_5000_c_31", "content", 31, dbDir)
	generate_indexpath(c, "tweets_10000_c_62", "content", 62, dbDir)
	generate_indexpath(c, "tweets_20000_c_125", "content", 125, dbDir)
	return
}
