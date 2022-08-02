# nebula-dump

简单可以读 rocksdb 并解码的工具

## 使用

工具依赖动态链接。

CentOS 下：

```
sudo yum install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm -y
sudo yum install snappy-devel lz4-devel libzstd-devel -y
```

Ubuntu 下：

```
sudo apt-get install -y\
  libsnappy-dev \
  zlib1g-dev \
  libbz2-dev \
  liblz4-dev \
  libzstd-dev
```

然后把二进制放在 path 下，如果使用 zsh，可以增加自动补全。

```
echo "autoload -U compinit; compinit" >> ~/.zshrc
nebula-dump completion zsh > "${fpath[1]}/_nebula-dump"

# 具体见
nebula-dump completion --help
```


## example

### meta

```bash
# get tags in meta
nebula-dump meta tags --path /data/bigdata/test/meta/nebula/0/data/

# get indexes in meta
nebula-dump meta indexes --path /data/bigdata/test/meta/nebula/0/data/

# get indexes with space id
nebula-dump meta indexes --path /data/bigdata/test/meta/nebula/0/data/ --space 1
nebula-dump meta indexes --path /data/bigdata/test/meta/nebula/0/data/ --space 1 --index 26
```

### storage

```bash
# dump and parse 
nebula-dump storage tags  --meta 192.168.15.30:9559  --path /data2/bigdata/test/storage/nebula/1/data/ --space 1 --vid 30786325636933
# do not parse
nebula-dump storage tags  --meta 192.168.15.30:9559  --path /data2/bigdata/test/storage/nebula/1/data/ --space 1 --vid 30786325636933 --raw

nebula-dump storage tags  --meta 192.168.15.30:9559  --path /data2/bigdata/test/storage/nebula/1/data/ --space 1 --part 3 --limit 100
nebula-dump storage tags  --meta 192.168.15.30:9559  --path /data2/bigdata/test/storage/nebula/1/data/ --space 1 --part 3 --limit 100 > tag.txt

# edge
nebula-dump storage edges --path /data/bigdata/test/storage/nebula/1/data/ --meta 192.168.15.30:9559  --space 1 --edge 18 --src 98 --limit 1
# or indirect edge
nebula-dump storage edges --path /data2/bigdata/test/storage/nebula/1/data/ --meta 192.168.15.30:9559  --space 1  --edge -18 --dst 211 --limit 1

# index
nebula-dump storage indexes  --meta 192.168.15.30:9559  --path /data2/bigdata/test/storage/nebula/1/data/ --space 1 --vid 30786325636933 --index 26
```

### utils

```bash
# get part id
nebula-dump utils partId --partsCount 96 --vid 30786325636933 --vidType int
nebula-dump utils partId --partsCount 96 --vid 30786325636933 --vidType string

# convert 
nebula-dump utils convert --key 石欣卉 --keyType string --toType bytes
nebula-dump utils convert --key 231,159,179,230,172,163,229,141,137 --keyType bytes --toType string

# scan some key
nebula-dump utils scan --path /data/bigdata/test/storage/nebula/1/data/ --limit 10 --prefix 1

# an user case, convert some key, and then scan.
nebula-dump utils convert --key __meta_cluster_id_key__ --keyType string --toType bytes
# 95,95,109,101,116,97,95,99,108,117,115,116,101,114,95,105,100,95,107,101,121,95,95
nebula-dump utils scan --path /data/bigdata/test/meta/nebula/0/data/ --limit 10 --prefix 95,95,109,101,116,97,95,99,108,117,115,116,101,114,95,105,100,95,107,101,121,95,95

# ingest sst files in tmp into test
# if there's no rocksd db engine, would create a new one.
nebula-dump utils ingest --sstPath tmp --toPath test
```
