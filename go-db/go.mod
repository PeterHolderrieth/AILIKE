module main

go 1.21.1

replace github.com/srmadden/godb => ./godb

replace github.com/xwb1989/sqlparser => github.com/manya-bansal/sqlparser v0.1.9

require (
	github.com/chzyer/readline v1.5.1
	github.com/srmadden/godb v0.0.0-00010101000000-000000000000
)

require github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2 // indirect

require (
	github.com/manya-bansal/sqlparser v0.1.9// indirect
	github.com/mitchellh/hashstructure/v2 v2.0.2 // indirect
	golang.org/x/exp v0.0.0-20230522175609-2e198f4a06a1 // indirect
	golang.org/x/sys v0.1.0 // indirect
)
