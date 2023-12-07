package main

import "fmt"


type fn (func(string) string)

func speedup_vary_db_size(tables []string, query_gen fn){
	return
}

func query_gen_1(table string) string{
	return "select sentiment, content, (content ailike the migration patterns of professor hair) sim from " + table + " order by sim desc, sentiment limit 5;"
}

func main() {
	tables := [...]string{"tweets", "tweets_2500", "tweets_5000", "tweets_10000", "tweets_20000"}
	query := query_gen_1(tables[0])
	fmt.Println(query)
	return
}
