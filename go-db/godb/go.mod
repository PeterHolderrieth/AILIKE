module github.com/srmadden/godb

go 1.19

replace github.com/xwb1989/sqlparser => github.com/manya-bansal/sqlparser v0.2.1

require (
	github.com/mitchellh/hashstructure/v2 v2.0.2
	github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2
	golang.org/x/exp v0.0.0-20230522175609-2e198f4a06a1
)

// exclude github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2 // indirect

// require github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2 // indirect
