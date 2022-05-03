package common

import "testing"

func TestIngest(t *testing.T) {
	err := Ingest("/home/Harris.chu/code/nebula-dump/sst/", "/home/Harris.chu/code/nebula-dump/tmp")
	if err != nil {
		t.Fatal(err)
	}

}
