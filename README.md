# nebula-dump

简单可以读 rocksdb 的工具。

## 需求

* 可以指定 string 的 key，做 get 或 scan 某个长度。
* 可以指定 byte[] 的 key，做 get 或 scan 某个长度。
* 按 partition 统计特定 prefix 的 key。

## example

```bash
./nebula-dump get --keyType string --key __meta_version__ --dir nebula/data/meta/nebula/0/data/
./nebula-dump get --keyType string --key __meta_version --dir /data/nebula/nebula_multiple_harris/nebula1/data/meta/nebula/0/data/ 
./nebula-dump scan --keyType bytes --start 7, --dir /data/nebula/nebula_multiple_harris/nebula1/data/storage/nebula/1/data/ --limit 2
./nebula-dump scan --keyType bytes --start 7,0,0,0 --dir /data/nebula/nebula_multiple_harris/nebula1/data/storage/nebula/1/data/ --count 2
./nebula-dump scan --keyType bytes --start 8,0,0,0 --dir /data/nebula/nebula_multiple_harris/nebula1/data/storage/nebula/1/data/ --count 2
./nebula-dump count --keyType bytes --prefixType 7 --partNum 40 --prefix=1,0,0 --dir /data/nebula/nebula_multiple_harris/nebula1/data/storage/nebula/1/data/ --count 2
```
