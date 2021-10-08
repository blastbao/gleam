// Disk-backed queue
package store

import (
	"io"
	"path"
	"time"
)

type DataStore interface {
	io.Writer					// Write()
	io.ReaderAt					// ReadAt()
	Destroy()					// 销毁
	LastWriteAt() time.Time		// 最近写入时间
	LastReadAt() time.Time		// 最近读取时间
}


// LocalFileDataStore 本地文件存储
type LocalFileDataStore struct {
	dir         string				// 目录
	name        string				// 名称
	store       *SingleFileStore	// dir/name.dat
	lastWriteAt time.Time			// 最新的写入时间
	lastReadAt  time.Time			//
}

func NewLocalFileDataStore(dir, name string) (ds *LocalFileDataStore) {
	ds = &LocalFileDataStore{
		dir:  dir,
		name: name,
		store: &SingleFileStore{
			Filename: path.Join(dir, name+".dat"),
		},
		lastWriteAt: time.Now(),
	}
	ds.store.init()
	return
}

func (ds *LocalFileDataStore) Write(data []byte) (int, error) {
	count, err := ds.store.Write(data)
	ds.lastWriteAt = time.Now()
	return count, err
}

func (ds *LocalFileDataStore) ReadAt(data []byte, offset int64) (int, error) {
	ds.lastReadAt = time.Now()
	return ds.store.ReadAt(data, offset)
}

func (ds *LocalFileDataStore) Destroy() {
	ds.store.Destroy()
}

func (ds *LocalFileDataStore) LastWriteAt() time.Time {
	return ds.lastWriteAt
}

func (ds *LocalFileDataStore) LastReadAt() time.Time {
	return ds.lastReadAt
}
