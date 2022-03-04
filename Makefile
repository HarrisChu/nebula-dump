
all: build

build:
	rm -rf nebula-dump
	CGO_CFLAGS="-I${rocksdb_path}/include" CGO_LDFLAGS="-L${rocksdb_path}"  go build