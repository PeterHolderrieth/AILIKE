# Running AILIKE

## Step 1: Update to the new parser
In the `go-db` run: 

```
go get main
go mod tidy
```

You might also need the following additional commands within `godb`:
```
TODO
```

## Step 1: Run the python server for generating embeddings

In the `/embedding` folder, start the python server by running:
  `python embedding.py`

## Step 2: Starting GoDB

In the `go-db` folder run:

- `go run main.go`

At this point, you will be able to start making ailike queries! You can skip to Step 4 if you already have data loaded.

## Step 3 (Optional): Load the data
You can use the \c command to load in a particular catalog. By default, GoDB will load in the tweets data catalog that matches the current embedding dimension configured in types.go.

You can load in a particular catalog by doing:
```
\c ../data/tweets/tweets_384/tweets_384.catalog
```

You can load data from a CSV into a table by doing:
```
\l tweets_mini ../data/tweets/tweets_mini.csv
\l tweets ../data/tweets/tweets.csv
```

NOTE: When loading data, you need to make sure the db configurable variables (TextEmbeddingDim and PageSize) match the parameters that were used to generate the .dat files. Furthermore, you need to make sure that the dimension of the vector within embeddings.py matches that of GoDB. You may need to restart the python server if they do not match.


After loading data into a table, you can create an index for the table using:
```
\i tabele col_name num_clusters index_type path/to/file; values for index_type are clustered and secondary
```

NOTE: Make sure col_name is an EmbeddedStringField.

examples:
```
\i tweets_clustered content 80 clustered ../data/tweets/tweets_384
\i tweets content 80 secondary ../data/tweets/tweets_384
\i tweets_mini_clustered content 10 clustered ../data/tweets/tweets_384
\i tweets_mini content 10 secondary ../data/tweets/tweets_384
```

## Step 4
Perform queries! Examples: 

- With a string literal: `select tweet_id, sentiment, (content ailike 'test string') sim from tweets_mini order by sim limit 5;`
- With a coloumn: `select tweet_id, sentiment, (content ailike content) sim from tweets_mini order by sim limit 5;`

You can use 'explain' to see the query plans. For example, you can compare the following:
explain select content, (content ailike 'hair migration patterns of professors') dist from tweets_mini order by dist limit 2;
explain select content, (content ailike 'hair migration patterns of professors') dist from tweets_mini_noindex order by dist limit 2;
explain select content, (content ailike 'hair migration patterns of professors') dist from tweets_mini_clustered order by dist limit 2;


Examples that should use index:
select tweet_id, sentiment, content, (content ailike 'hair migration patterns of professors') dist from tweets_mini order by dist limit 2;
select tweet_id, sentiment, content, (content ailike 'I am feeling really tired') sim from tweets_mini order by sim desc, sentiment limit 5;
select max(content ailike 'I am feeling really tired') from tweets_mini;

Examples that could use index, but don't:
explain select t1.tweet_id, t1.sentiment, max(t1.content ailike t2.content) from tweets_mini as t1 join tweets_mini as t2 on t1.sentiment = t2.sentiment group by t1.tweet_id, t1.sentiment;

Examples that should not use index:
select count(*) from tweets_mini;
select tweet_id, sentiment, content, (content ailike 'I am feeling really tired') sim from tweets_mini order by sentiment, sim desc limit 5;
select tweet_id, sentiment, content, (content ailike 'I am feeling really tired') sim from tweets_mini where sentiment = 'enthusiasm' order by sim desc limit 5;
select * from tweets_mini limit 10;
select * from tweets_mini where sentiment = 'enthusiasm' limit 10;
select max(content ailike 'I am feeling really tired'), min(content ailike 'I am feeling really energized') from tweets_mini;

How to count numbers of pages per cluster:
select centroidid, count(indexpageno) from secondary__tweets_mini__content__mapping group by centroidid;