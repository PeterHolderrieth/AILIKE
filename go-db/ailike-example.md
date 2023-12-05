# Running an AILIKE example


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

## Step 3: Load the data

If using random projections, load the data by running:
```
\c ../data/tweets/tweets_32/tweets_32.catalog
```

If using the full embeddings, load the data by running:
```
\c ../data/tweets/tweets_768/tweets_768.catalog
```

To create an embedding for a table, use (make sure that have loaded the data first!):
```
\i tabele col_name num_clusters path_to_file
```

NOTE: When loading data, you need to make sure the db configurable variables (UseRandomProj and PageSize) match the parameters that were used to generate the .dat files. Furthermore, you need to make sure that the value of RANDOM_PROJ within embedding.py matches that of UseRandomProj. You may need to restart the python server if they do not match.

## Step 4
Perform queries! Examples: 

- With a string literal: `select tweet_id, sentiment, (content ailike 'test string') sim from tweets_mini order by sim limit 5;`
- With a coloumn: `select tweet_id, sentiment, (content ailike content) sim from tweets_mini order by sim limit 5;`

Examples that should use index:
select tweet_id, sentiment, content, (content ailike 'hair migration patterns of professors') sim from tweets_mini order by sim desc limit 5;
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
