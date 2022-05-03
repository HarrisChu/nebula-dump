package common

import "testing"

func TestRocksdb(t *testing.T) {
	e, err := NewRocksDbEngine("/home/Harris.chu/workspace/nebula-docker-compose/data/meta0/nebula/0/data")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(e)
	t.Fatal(1)
}
