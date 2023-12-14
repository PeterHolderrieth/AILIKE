module github.com/srmadden/ailike

go 1.21.1

replace github.com/xwb1989/sqlparser => github.com/manya-bansal/sqlparser v0.2.2

require (
	github.com/mitchellh/hashstructure/v2 v2.0.2
	github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2
	golang.org/x/exp v0.0.0-20231206192017-f3f8817b8deb
)

// exclude github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2 // indirect

// require github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2 // indirect
