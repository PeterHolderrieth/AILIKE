# Running an AILIKE example


## Step 1: Update to the new parser
In the `go-db` run: 

```
go get main
go mod tidy
```

## Step 1: Generate embeddings 

In the embedding folder, run:
  `python embedding.py`

## Step 2: Starting GoDB

In the `go-db` folder run:

- `go run main.go`
- Load the catalog `\c godb/catalog_text.txt`

## Step 3
Perform queries! Examples: 

- With a string literal: `select name, age, (biography ailike 'test string') sim from t_text order by sim limit 5;`
- With a coloumn: `select name, age, (biography ailike biography) sim from t_text order by sim limit 5;`
