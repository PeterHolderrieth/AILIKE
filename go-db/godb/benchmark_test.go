package godb

import (
	"fmt"
	"strconv"
	"testing"
)

func TestBenchmarkingInfra(t *testing.T) {
	var config = BenchMetaData{
		catalog:   "catalog_text.txt",
		dbDir:     "./",
		outputDir: "./benchmark_results",
		bpSize:    10,
		save:      true}
	time, err := BenchmarkingInfra("test", "select biography, 'doctor' ailike biography from t_text", config)
	if err != nil {
		t.Errorf("%s", err.Error())
	}
	fmt.Println("Time taken = ", time)
}

type fn (func(string) string)

func speedup_vary_db_size(tables []string, catalog string, path string, query_gen fn) {
	for _, table := range tables {
		var config = newBenchMetaData(catalog, path, 1000,
			"./benchmark_results/var_db_size", true)
		time, err := BenchmarkingInfra(table, query_gen(table), config)
		if err == nil {
		}
		fmt.Println("Time taken for ", table, "=", time)
	}
	return
}

func speedup_vary_num_cluster(tables []string, catalog string, path string, query_gen fn) {
	for _, table := range tables {
		var config = newBenchMetaData(catalog, path, 1000,
			"./benchmark_results/var_num_cluster", true)
		time, err := BenchmarkingInfra(table, query_gen(table), config)
		if err == nil {
		}
		fmt.Println("Time taken for ", table, "=", time)
	}
	return
}

func query_gen_1(table string) string {
	return "select sentiment, content, (content ailike 'the migration patterns of professor hair') sim from " + table + " order by sim desc, sentiment limit 5;"
}

func TestVaryDBSize(t *testing.T) {
	catalog := "tweets_384.catalog"
	catalog_path := "/Users/manyab/AILIKE/data/tweets/tweets_384"
	tables := []string{"tweets", "tweets_2500",
		"tweets_5000", "tweets_10000", "tweets_20000",
		"tweets_c_250", "tweets_2500_c_16", "tweets_5000_c_31",
		"tweets_10000_c_62", "tweets_20000_c_125",
	}
	speedup_vary_db_size(tables, catalog, catalog_path, query_gen_1);
	return
}

func query_gen_2(table string) string {
	return "select max(content ailike 'I am feeling really tired') from " + table + ";"
}

func speedup_vary_second_query(tables []string, catalog string, path string, query_gen fn) {
	for _, table := range tables {
		var config = newBenchMetaData(catalog, path, 1000,
			"./benchmark_results/var_agg_query", true)
		time, err := BenchmarkingInfra(table, query_gen(table), config)
		if err == nil {
		}
		fmt.Println("Time taken for ", table, "=", time)
	}
	return
}

func TestVaryNewQuery(t *testing.T) {
	catalog := "tweets_384.catalog"
	catalog_path := "/Users/manyab/AILIKE/data/tweets/tweets_384"
	tables := []string{"tweets", "tweets_2500",
		"tweets_5000", "tweets_10000", "tweets_20000",
		"tweets_c_250", "tweets_2500_c_16", "tweets_5000_c_31",
		"tweets_10000_c_62", "tweets_20000_c_125",
	}
	speedup_vary_second_query(tables, catalog, catalog_path, query_gen_2);
	return
}

func TestVaryNumCluster(t *testing.T) {
	catalog := "tweets_384.catalog"
	catalog_path := "/Users/manyab/AILIKE/data/tweets/tweets_384"
	tables := []string{"tweets", "tweets_c_50",
	  "tweets_c_100", "tweets_c_250",
		"tweets_c_500",
	}
	speedup_vary_num_cluster(tables, catalog, catalog_path, query_gen_1);
	return
}

func speedup_vary_n(tables []string, N []int, catalog string, path string, query_gen (func(string, int) string)) {
	for _, table := range tables {
		for _, n := range N {
		var config = newBenchMetaData(catalog, path, 1000,
			"./benchmark_results/var_n", true)
		time, err := BenchmarkingInfra(table + "_" + strconv.Itoa(n), query_gen(table, n), config)
		if err == nil {
		}
		fmt.Println("Time taken for ", table, " with ", n, "=", time)
		}
	}
	return
}

func query_gen_n(table string, n int) string {
	return "select sentiment, content, (content ailike 'the migration patterns of professor hair') sim from " + table + " order by sim desc, sentiment limit " + strconv.Itoa(n) + ";"
}

func TestVaryLimit(t *testing.T) {
	catalog := "tweets_384.catalog"
	catalog_path := "/Users/manyab/AILIKE/data/tweets/tweets_384"
	tables := []string{"tweets", "tweets_c_250"}
	N := []int{2, 4, 8, 32, 64, 128, 256};
	speedup_vary_n(tables, N, catalog, catalog_path, query_gen_n);
	return
}

func speedup_vary_probe(tables []string, N []int, catalog string, path string, query_gen fn) {
	for _, table := range tables {
		for _, n := range N {
		var config = newBenchMetaData(catalog, path, 1000,
			"./benchmark_results/var_probe", true)
		DefaultProbe = n;
		time, err := BenchmarkingInfra(table + "_" + strconv.Itoa(n), query_gen(table), config)
		if err != nil {
			fmt.Println("Failed for ", table , " with probe = ", n)
			continue 
		}
		fmt.Println("Time taken for ", table, " with ", n, "=", time)
		}
	}
}

func query_gen_probe(table string) string {
	return "select sentiment, content, (content ailike 'the migration patterns of professor hair') sim from " + table + " order by sim desc, sentiment limit 20;"
}

func TestDefaultProbe(t *testing.T) {
	catalog := "tweets_384.catalog"
	catalog_path := "/Users/manyab/AILIKE/data/tweets/tweets_384"
	tables := []string{"tweets", "tweets_c_250"}
	N := []int{1, 2, 3, 4, 5, 6, 7};
	speedup_vary_probe(tables, N, catalog, catalog_path, query_gen_probe);
	return
}
