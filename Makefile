
all: build

rocksdb_version?=6.27.3
jobs?=8

prepare:
	@if [ ! -d rocksdb ]; then \
		git clone --branch v${rocksdb_version} https://github.com/facebook/rocksdb.git; \
		cd rocksdb ;\
		make static_lib -j ${jobs};\
	fi

build: prepare
	rm -rf ./nebula-dump
	CGO_CFLAGS="-I${PWD}/rocksdb/include" \
	CGO_LDFLAGS="-L${PWD}/rocksdb -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lbz2"  \
	go build

build-docker:
	docker build -t nebula-dump .

zsh-complet:
	nebula-dump completion zsh > "$${fpath[1]}/_nebula-dump"
	source ~/.zshrc