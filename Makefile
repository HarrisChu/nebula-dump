
all: build

#rocksdb_version?=7.5.3
nebula_branch?=master
jobs?=8
current_dir=$(shell pwd)

prepare:
	@if [ ! -d third-party ]; then \
		mkdir third-party; \
		git clone --branch ${nebula_branch} https://github.com/vesoft-inc/nebula; \
		nebula/third-party/install-third-party.sh --prefix=${current_dir}/third-party; \
		rm -rf nebula; \
	fi

build: prepare
	rm -rf ./nebula-dump
	CGO_CFLAGS="-I${PWD}/third-party/include" \
	CGO_LDFLAGS="-L${PWD}/third-party/lib -L${PWD}/third-party/lib64 -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lbz2"  \
	go build -ldflags '-linkmode external -extldflags "-L/usr/local/lib -Wl,-Bstatic -lrocksdb -lsnappy -lstdc++ -lm -lz -lbz2 -llz4 -lzstd -Wl,-Bdynamic"'

build-docker:
	docker build -t nebula-dump .

zsh-complet:
	nebula-dump completion zsh > "$${fpath[1]}/_nebula-dump"
	source ~/.zshrc
