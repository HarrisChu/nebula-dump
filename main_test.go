package main

import "testing"

func TestA(t *testing.T) {
	t.Log(255 >> 8)
	t.Fatal(1)
}
