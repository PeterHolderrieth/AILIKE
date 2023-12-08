package main

import (
	"fmt"

	"github.com/srmadden/godb"
)

type fn (func(string) string)

func speedup_vary_db_size(tables []string, catalog string, path string, query_gen fn) {
	for _, table := range tables {
		var config = godb.newBenchMetaData(catalog, path,
			"./benchmark_results/vary_db_size", 1000, true)
		time, err := godb.BenchmarkingInfra(table, query_gen(table), config)
		if err == nil {
			fmt.Println(err)
		}
		fmt.Println("Time taken = ", time)
	}
	return
}

func query_gen_1(table string) string {
	return "select sentiment, content, (content ailike the migration patterns of professor hair) sim from " + table + " order by sim desc, sentiment limit 5;"
}

func main() {
	catalog := "/Users/manyab/AILIKE/data/tweets/"
	catalog_path := "/Users/manyab/AILIKE/data/tweets/tweets_384"
	tables := []string{"tweets", "tweets_2500",
		"tweets_5000", "tweets_10000", "tweets_20000",
		"tweets_c_250", "tweets_2500_c_16", "tweets_5000_c_31",
		"tweets_10000_c_62", "tweets_20000_c_125",
	}
	query := query_gen_1(tables[0])
	speedup_vary_db_size(tables, catalog, catalog_path, query_gen_1);
	fmt.Println(query)
	return
}
