package godb

import (
	"fmt"
	"testing"
)


func TestAlikeParse(t *testing.T) {
	var query = "select col1 ailike col2 from t"

	bp := NewBufferPool(10)
	err := MakeTestDatabaseEasy(bp)

	if err != nil {
		t.Errorf("failed to create test database, %s", err.Error())
		return
	}

	c, err := NewCatalogFromFile("catalog.txt", bp, "./")
	if err != nil {
		t.Errorf("failed load catalog, %s", err.Error())
		return
	}

	tid := NewTID()
	bp.BeginTransaction(tid)

	qType, plan, err := Parse(c, query)

	fmt.Println(plan)

	if err != nil {
		t.Errorf("failed to parse, q=%s, %s", query, err.Error())
		return
	}

	if plan == nil {
		t.Errorf("plan was nil")
		return
	}
	if qType != IteratorType {
		t.Errorf("Not iterator type")
	}
}