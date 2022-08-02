package pkg

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"time"

	"github.com/harrischu/nebula-dump/pkg/common"
	gorocksdb "github.com/linxGnu/grocksdb"
)

type (
	// Dumper dumper
	MyDumper interface {
		Open(dir string) error
		Get(ctx context.Context, key []byte) (kv *common.KV, err error)
		ScanByRange(ctx context.Context, start, end []byte) (kvs []*common.KV, err error)
		ScanByCount(ctx context.Context, start []byte, count int) (kvs []*common.KV, err error)
		Count(ctx context.Context, partNum, prefixType int, prefix []byte) (int64, error)
	}

	RocksdbDump struct {
		db *gorocksdb.DB
	}
)

func NewDumper() MyDumper {
	return &RocksdbDump{}
}

func (d *RocksdbDump) Open(dir string) error {
	db, err := gorocksdb.OpenDbForReadOnly(gorocksdb.NewDefaultOptions(), dir, false)
	if err != nil {
		return err
	}
	d.db = db
	return nil

}

func (d *RocksdbDump) runWithCtx(ctx context.Context, f func() (interface{}, error)) (interface{}, error) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	resultCh := make(chan interface{})
	errCh := make(chan error)
	defer close(resultCh)
	defer close(errCh)
	go func() {
		r, err := f()
		if err != nil {
			errCh <- err
		} else {
			resultCh <- r
		}
	}()

	for {
		select {
		case <-runCtx.Done():
			return nil, fmt.Errorf("cancelled")
		case c := <-resultCh:
			return c, nil
		case err := <-errCh:
			return nil, err
		default:
			time.Sleep(300 * time.Millisecond)
		}
	}

}

func (d *RocksdbDump) Get(ctx context.Context, key []byte) (*common.KV, error) {
	r, err := d.runWithCtx(ctx, func() (interface{}, error) {
		return d.get(key)
	})
	if err != nil {
		return nil, err
	}
	return r.(*common.KV), nil
}

func (d *RocksdbDump) ScanByRange(ctx context.Context, start, end []byte) (kvs []*common.KV, err error) {
	r, err := d.runWithCtx(ctx, func() (interface{}, error) {
		iter := d.db.NewIterator(gorocksdb.NewDefaultReadOptions())
		defer iter.Close()
		if start == nil {
			iter.SeekToFirst()
		} else {
			iter.Seek(start)
		}

		for ; iter.Valid(); iter.Next() {
			if bytes.Compare(iter.Key().Data(), end) > -1 {
				break
			}
			kv := common.NewKV(iter.Key().Data(), iter.Value().Data())
			kvs = append(kvs, kv)
		}
		return kvs, nil
	})
	if err != nil {
		return nil, err
	}
	return r.([]*common.KV), nil
}

func (d *RocksdbDump) ScanByCount(ctx context.Context, start []byte, count int) (kvs []*common.KV, err error) {
	r, err := d.runWithCtx(ctx, func() (interface{}, error) {
		iter := d.db.NewIterator(gorocksdb.NewDefaultReadOptions())
		defer iter.Close()
		if start == nil {
			iter.SeekToFirst()
		} else {
			iter.Seek(start)
		}
		for i := 0; i < count && iter.Valid(); i++ {
			kv := common.NewKV(iter.Key().Data(), iter.Value().Data())
			kvs = append(kvs, kv)
			iter.Next()
		}
		return kvs, nil
	})
	if err != nil {
		return nil, err
	}
	return r.([]*common.KV), nil

}

func (d *RocksdbDump) Count(ctx context.Context, partNum, prefixType int, prefix []byte) (int64, error) {
	r, err := d.runWithCtx(ctx, func() (interface{}, error) {
		sum := int64(0)
		wg := sync.WaitGroup{}
		wg.Add(partNum)
		for i := 0; i < partNum; i++ {
			go func(sum *int64, partId int) {
				var data []byte
				prefixBuf := bytes.Buffer{}
				prefixPart := int32((partId << 8) | prefixType)
				if err := common.ConvertIntToBytes(&prefixPart, &data, common.ByteOrder); err != nil {
					panic(err)
				}
				prefixBuf.Write(data)
				prefixBuf.Write(prefix)
				s := count(d.db, prefixBuf.Bytes())
				atomic.AddInt64(sum, s)
				wg.Done()
			}(&sum, i)
		}
		wg.Wait()
		return sum, nil
	})
	if err != nil {
		return 0, err
	}
	return r.(int64), nil
}

func (d *RocksdbDump) get(key []byte) (kvs *common.KV, err error) {
	iter := d.db.NewIterator(gorocksdb.NewDefaultReadOptions())
	defer iter.Close()
	iter.Seek(key)
	if iter.Valid() && bytes.Equal(iter.Key().Data(), key) {
		kv := common.NewKV(iter.Key().Data(), iter.Value().Data())
		return kv, nil
	}
	return nil, nil
}

func count(db *gorocksdb.DB, prefix []byte) int64 {
	count := int64(0)
	iter := db.NewIterator(gorocksdb.NewDefaultReadOptions())
	defer iter.Close()
	iter.Seek(prefix)
	for it := iter; it.Valid(); it.Next() {
		if !bytes.Contains(it.Key().Data(), prefix) {
			break
		} else {
			count++
		}
	}
	return count
}
