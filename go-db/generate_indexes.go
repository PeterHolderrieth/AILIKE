package main

import (
	"fmt"

	"github.com/srmadden/godb"
)

func generate_indexpath(c *godb.Catalog, table string, column string, clusters int, path string, clustured bool) {
	bp := godb.NewBufferPool(1000)
	hf, err := c.GetTable(table)
	fmt.Println("Generating index for ", table)
	_, err = godb.ConstructNNIndexFileFromHeapFile(hf.(*godb.HeapFile), column, clusters, clustured,
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
	generate_indexpath(c, "tweets_c_200", "content", 250, dbDir, false)
	generate_indexpath(c, "tweets_c_100", "content", 100, dbDir, false)
	generate_indexpath(c, "tweets_c_80", "content", 100, dbDir, false)
	generate_indexpath(c, "tweets_c_40", "content", 100, dbDir, false)
	generate_indexpath(c, "tweets_2500_c_5", "content", 16, dbDir, false)
	generate_indexpath(c, "tweets_5000_c_10", "content", 31, dbDir, false)
	generate_indexpath(c, "tweets_10000_c_20", "content", 62, dbDir, false)
	generate_indexpath(c, "tweets_20000_c_40", "content", 125, dbDir, false)


	generate_indexpath(c, "clustered_tweets_c_200", "content", 250, dbDir, true)
	generate_indexpath(c, "clustered_tweets_c_100", "content", 100, dbDir, true)
	generate_indexpath(c, "clustered_tweets_c_80", "content", 100, dbDir, true)
	generate_indexpath(c, "clustered_tweets_c_40", "content", 100, dbDir, true)
	return
}
