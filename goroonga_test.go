package goroonga

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"
)

func TestGoroonga(t *testing.T) {
	Init()
	defer Fin()

	ctx, err := NewContext()
	if err != nil {
		t.FailNow()
	}
	defer ctx.Fin()

	tempDirPath, err := ioutil.TempDir("", "goroonga-test")
	defer os.Remove(tempDirPath)

	testDbPath := tempDirPath + "/db"

	database, err := ctx.CreateDatabase(testDbPath)
	if err != nil {
		t.FailNow()
	}
	defer ctx.CloseDatabase(database)

	response, err := ctx.Query("status")
	if err != nil {
		t.FailNow()
	}

	var f interface{}
	err = json.Unmarshal(response, &f)
	if err != nil {
		t.FailNow()
	}
}
