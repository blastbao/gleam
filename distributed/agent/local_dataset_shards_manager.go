package agent

import (
	"fmt"
	"sync"
	"time"

	"github.com/chrislusf/gleam/distributed/store"
)



// LocalDatasetShardsManager 本地数据集分配管理器
type LocalDatasetShardsManager struct {
	sync.Mutex
	dir            string
	port           int
	name2Store     map[string]store.DataStore
	name2StoreCond *sync.Cond	// 条件变量
}

func NewLocalDatasetShardsManager(dir string, port int) *LocalDatasetShardsManager {
	m := &LocalDatasetShardsManager{
		dir:        dir,
		port:       port,
		name2Store: make(map[string]store.DataStore),
	}
	m.name2StoreCond = sync.NewCond(m)
	return m
}

func (m *LocalDatasetShardsManager) doDelete(name string) {

	// println("deleting from LocalDatasetShardsManager:", name)

	ds, ok := m.name2Store[name]
	if !ok {
		return
	}

	delete(m.name2Store, name)

	ds.Destroy()
}

func (m *LocalDatasetShardsManager) DeleteNamedDatasetShard(name string) {

	// println("locking LocalDatasetShardsManager to delete", name)

	m.Lock()
	defer m.Unlock()

	// println("locked LocalDatasetShardsManager to delete", name)

	m.doDelete(name)

}

func (m *LocalDatasetShardsManager) CreateNamedDatasetShard(name string) store.DataStore {

	m.Lock()
	defer m.Unlock()

	// 如果已存在，就删除
	_, ok := m.name2Store[name]
	if ok {
		m.doDelete(name)
	}

	// 创建新的数据
	s := store.NewLocalFileDataStore(m.dir, fmt.Sprintf("%s-%d", name, m.port))

	// 保存
	m.name2Store[name] = s

	// println(name, "is broadcasting...")
	m.name2StoreCond.Broadcast()

	return s

}

func (m *LocalDatasetShardsManager) WaitForNamedDatasetShard(name string) store.DataStore {

	m.Lock()
	defer m.Unlock()

	for {
		// 检查 name 的数据集是否存在
		if ds, ok := m.name2Store[name]; ok {
			return ds
		}
		// println(name, "is waiting to read...")
		// 不存在则等待
		m.name2StoreCond.Wait()
	}

}

// purge executor status older than 24 hours to save memory
func (m *LocalDatasetShardsManager) purgeExpiredEntries() {
	for {
		func() {
			m.Lock()

			// 24 Hour
			cutoverLimit := time.Now().Add(-24 * time.Hour)

			// 取出读写时间在 24 hour 之前的数据集
			var oldShardNames []string
			for name, ds := range m.name2Store {
				if ds.LastWriteAt().Before(cutoverLimit) && ds.LastReadAt().Before(cutoverLimit) {
					println("purging dataset", name, "last write:", ds.LastWriteAt().String(), "last read:", ds.LastReadAt().String())
					oldShardNames = append(oldShardNames, name)
				}
			}

			// 逐个删除这些过期的数据集
			for _, name := range oldShardNames {
				m.doDelete(name)
			}

			m.Unlock()

			// 每小时执行一次
			time.Sleep(1 * time.Hour)
		}()
	}
}
