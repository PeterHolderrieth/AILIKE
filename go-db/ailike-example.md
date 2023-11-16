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


NOTE: When loading data, you need to make sure the db configurable variables (UseRandomProj and PageSize) match the parameters that were used to generate the .dat files. Furthermore, you need to make sure that the value of RANDOM_PROJ within embedding.py matches that of UseRandomProj. You may need to restart the python server if they do not match.

## Step 4
Perform queries! Examples: 

- With a string literal: `select tweet_id, sentiment, (content ailike 'test string') sim from tweets_mini order by sim limit 5;`
- With a coloumn: `select tweet_id, sentiment, (content ailike content) sim from tweets_mini order by sim limit 5;`
