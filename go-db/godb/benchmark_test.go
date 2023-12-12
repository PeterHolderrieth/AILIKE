package godb

import (
	"fmt"
	"os"
	"strconv"
	"testing"
)

var warmup_iter = 2

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

// Benchmarking Functions
type fn (func(string) string)

func varyTableOnly(tables []string, catalog string, path string, query_gen []fn, dataDir string, save bool) {

	timing_csv_path := dataDir + "/times.csv"
	timing_csv, err := os.OpenFile(timing_csv_path, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		fmt.Println("Failed to create out file, exiting...")
		return
	}
	for _, table := range tables {
		time := int64(0)
		for _, queries := range query_gen {
			var config = newBenchMetaData(catalog, path, 1000,
				dataDir, save, warmup_iter)
			fmt.Println("Starting ", table, " with ", queries(table))
			time_taken, err := BenchmarkingInfra(table, queries(table), config)
			if err != nil {
				fmt.Println("-->Failed to run ", queries(table))
			}
			fmt.Println("Done!")
			time += time_taken
		}
		fmt.Println("Time taken for ", table, " = ", time/int64(len(query_gen)))
		fmt.Fprintf(timing_csv, "%s, %d\n", table, time/int64(len(query_gen)))
	}
	timing_csv.Close()
}

func query_gen(sim_string string, num int) func(string) string {
	lambda := func(table string) string {
		return "select tweet_id, content, (content ailike '" + sim_string + "') sim from " + table + " order by sim desc, tweet_id limit " + strconv.Itoa(num)
	}
	return lambda
}

func query_gen_agg(sim_string string) func(string) string {
	lambda := func(table string) string {
		return "select max(tweet_id), max(content ailike '" + sim_string + "') from " + table + ";"
	}
	return lambda
}

func TestVaryDBSize(t *testing.T) {
	catalog := "new.catalog"
	catalog_path := "/Users/manyab/AILIKE/data/tweets/tweets_384"
	tables := []string{
		"tweets", "tweets_2500",
		"tweets_5000", "tweets_10000", "tweets_20000",
		"tweets_2500_c_5", "tweets_5000_c_10", "tweets_10000_c_20",
		"tweets_20000_c_40", "tweets_c_80",
	}
	// varyTableOnly(tables, catalog, catalog_path,
	// 	[]fn{query_gen_agg("the migration patterns of professor hair"),
	// 		query_gen("the migration patterns of professor hair", 5),
	// 		query_gen_agg("I am so happy"),
	// 		query_gen("I am so happy", 5),
	// 		query_gen_agg("grad school is amazing"),
	// 		query_gen("grad school is amazing", 5)},
	// 	"./benchmark_results/var_db_size", false)

	varyTableOnly(tables, catalog, catalog_path,
		[]fn{query_gen("the migration patterns of professor hair", 1),
			query_gen("the migration patterns of professor hair", 5),
			query_gen("I am so happy", 1), 
			query_gen("I am so happy", 5), 
			query_gen("grad school is amazing", 1), 
			query_gen("grad school is amazing", 5)},
		"./benchmark_results/var_db_size", false)
}

func TestVaryNewQuery(t *testing.T) {
	catalog := "tweets_384.catalog"
	catalog_path := "/Users/manyab/AILIKE/data/tweets/tweets_384"
	tables := []string{"tweets", "tweets_2500",
		"tweets_5000", "tweets_10000", "tweets_20000",
		"tweets_c_250", "tweets_2500_c_16", "tweets_5000_c_31",
		"tweets_10000_c_62", "tweets_20000_c_125",
	}
	varyTableOnly(tables, catalog, catalog_path,
		[]fn{query_gen_agg("the migration patterns of professor hair"),
			query_gen_agg("I am so happy"),
			query_gen_agg("I want to do to the doctor"),
			query_gen_agg("I am a grad student")},
		"./benchmark_results/var_agg_query", false)
}

func TestVaryNumCluster(t *testing.T) {
	catalog := "new.catalog"
	catalog_path := "/Users/manyab/AILIKE/data/tweets/tweets_384"
	tables := []string{"tweets", "tweets_c_40",
		"tweets_c_80", "tweets_c_100",
		"tweets_c_200",
	}
	DefaultProbe = 5
	varyTableOnly(tables, catalog, catalog_path,
		[]fn{query_gen("the migration patterns of professor hair", 1),
			query_gen("the migration patterns of professor hair", 5),
			query_gen("I am so happy", 1), 
			query_gen("I am so happy", 5), 
			query_gen("grad school is amazing", 1), 
			query_gen("grad school is amazing", 5)},
		"./benchmark_results/var_num_cluster", false)
}


type fn_n (func(string, int) string)

func varyTableAndN(tables []string, N []int, catalog string, path string, query_gen []fn_n, dataDir string, save bool) {

	timing_csv_path := dataDir + "/times.csv"
	timing_csv, err := os.OpenFile(timing_csv_path, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		fmt.Println("Failed to create out file, exiting...")
		return
	}

	for _, table := range tables {
		time := int64(0)
		for _, n := range N {
			for i, queries := range query_gen {
				var config = newBenchMetaData(catalog, path, 1000,
					dataDir, save, warmup_iter)
				DefaultProbe = n/3 + 1;
				time_taken, err := BenchmarkingInfra(table+"_"+strconv.Itoa(n)+"_"+strconv.Itoa(i), queries(table, n), config)
				if err != nil {
					fmt.Println("Failed to run ", queries(table, n))
					fmt.Println(err)
				}
				time += time_taken
			}
			fmt.Println("Time taken for ", table, " = ", time/int64(len(query_gen)))
			fmt.Fprintf(timing_csv, "%s, %v\n", table+"_"+strconv.Itoa(n), time/int64(len(query_gen)))
		}
	}
	timing_csv.Close()
}

func query_gen_limit(sim_string string) fn_n {
		lambda := func(table string, limit int) string {
			return "select tweet_id, content, (content ailike '" + sim_string + "') sim from " + table + " order by sim desc, tweet_id limit " + strconv.Itoa(limit)
		}
		return lambda
}

func TestVaryLimit(t *testing.T) {
	catalog := "new.catalog"
	catalog_path := "/Users/manyab/AILIKE/data/tweets/tweets_384"
	tables := []string{"tweets", "tweets_c_80",
		"clustered_tweets_c_80",
	}
	N := []int{1, 8, 64, 512, 4096, 8000, 16000, 40000}
	varyTableAndN(tables, N, catalog, catalog_path,
		[]fn_n{query_gen_limit("the migration patterns of professor hair"),
		query_gen_limit("I am so happy"),
		query_gen_limit("grad school is amazing")},
		"./benchmark_results/var_n", false)
}

func varyTableAndSetVar(tables []string, N []int, catalog string, path string, query_gen []fn, dataDir string, set_var func(int)) {

	timing_csv_path := dataDir + "/times.csv"
	timing_csv, err := os.OpenFile(timing_csv_path, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		fmt.Println("Failed to create out file, exiting...")
		return
	}

	for _, table := range tables {
		time := int64(0)
		for _, n := range N {
			for i, queries := range query_gen {
				var config = newBenchMetaData(catalog, path, 1000,
					dataDir, true, warmup_iter)
				set_var(n)
				time_taken, err := BenchmarkingInfra(table+"_"+strconv.Itoa(n)+"_"+strconv.Itoa(i), queries(table), config)
				if err != nil {
					fmt.Println("Failed to run ", queries(table))
					fmt.Println(err)
				}
				time += time_taken
			}
			fmt.Println("Time taken for ", table, " = ", time/int64(len(query_gen)))
			fmt.Fprintf(timing_csv, "%s, %v\n", table+"_"+strconv.Itoa(n), time/int64(len(query_gen)))
		}
	}
	timing_csv.Close()
}

func set_default_probe(n int) {
	DefaultProbe = n + 10
}

func TestDefaultProbe(t *testing.T) {
	catalog := "new.catalog"
	catalog_path := "/Users/manyab/AILIKE/data/tweets/tweets_384"
	tables := []string{"tweets", "tweets_c_80"}
	N := []int{2, 4, 8, 16}
	varyTableAndSetVar(tables, N, catalog, catalog_path,
		[]fn{query_gen("the migration patterns of professor hair", 1),
		query_gen("the migration patterns of professor hair", 5),
		query_gen("the migration patterns of professor hair", 10), 
		query_gen("I am so happy", 1),
		query_gen("I am so happy", 5),
		query_gen("I am so happy", 10)},
		"./benchmark_results/var_probe", set_default_probe)
}

func set_buffer_pool_strategy(n int) {
	if n == 0{
		USE_EVICT_QUEUE = false
		return
	}
	if n == 1{
		USE_EVICT_QUEUE = true
		return
	}
	panic("unreachable")
}


func varyBufferPoolAndVary(tables []string, N []int, catalog string, path string, query_gen []fn_n, dataDir string) {

	timing_csv_path := dataDir + "/times.csv"
	timing_csv, err := os.OpenFile(timing_csv_path, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		fmt.Println("Failed to create out file, exiting...")
		return
	}

	for _, table := range tables {
		time := int64(0)
		for _, p := range []int{0, 1}{
		for _, n := range N {
			for i, queries := range query_gen {
				var config = newBenchMetaData(catalog, path, 1000,
					dataDir, true, warmup_iter)
				if p == 1{
					USE_EVICT_QUEUE = true
				}else{
					USE_EVICT_QUEUE = false
				}
				time_taken, err := BenchmarkingInfra(table+"_"+strconv.Itoa(p)+"_"+strconv.Itoa(n)+"_"+strconv.Itoa(i), queries(table, i), config)
				if err != nil {
					fmt.Println("Failed to run ", queries(table, i))
					fmt.Println(err)
				}
				time += time_taken
			}
			fmt.Println("Time taken for ", table, " = ", time/int64(len(query_gen)))
			fmt.Fprintf(timing_csv, "%s, %v\n", table+"_"+strconv.Itoa(p)+"_"+strconv.Itoa(n), time/int64(len(query_gen)))
		}
		}
	}
	timing_csv.Close()
}

func TestBufferPoolStrategy(t *testing.T) {
	catalog := "new.catalog"
	catalog_path := "/Users/manyab/AILIKE/data/tweets/tweets_384"
	tables := []string{"tweets",
		"tweets_c_80", "clustered_tweets_c_80",
	}

	N := []int{1, 8, 64, 512, 4096}
	varyBufferPoolAndVary(tables, N, catalog, catalog_path,
		[]fn_n{query_gen_limit("the migration patterns of professor hair"),
		query_gen_limit("I am so happy"),
		query_gen_limit("grad school is amazing")},
		"./benchmark_results/buf_pool")
}