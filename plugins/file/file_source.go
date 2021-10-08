package file

import (
	"fmt"
	"io"
	"log"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/chrislusf/gleam/filesystem"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

type FileSource struct {
	folder         string
	fileBaseName   string
	hasWildcard    bool
	Path           string
	HasHeader      bool
	PartitionCount int
	FileType       string
	Fields         []string

	prefix string
}

// Generate generates data shard info, partitions them via round robin, and reads each shard on each executor.
func (q *FileSource) Generate(f *flow.Flow) *flow.Dataset {

	return q.genShardInfos(f).RoundRobin(q.prefix, q.PartitionCount).Map(q.prefix+".Read", registeredMapperReadShard)
}

// SetHasHeader sets whether the data contains header
func (q *FileSource) SetHasHeader(hasHeader bool) *FileSource {
	q.HasHeader = hasHeader
	return q
}

// Select selects fields that can be pushed down to data sources supporting columnar reads
// TODO adjust FileSource api to denote which data source can support columnar reads
func (q *FileSource) Select(fields ...string) *FileSource {
	q.Fields = fields
	return q
}

// New creates a FileSource based on a file name.
// The base file name can have "*", "?" pattern denoting a list of file names.
//
//
//
func newFileSource(fileType, fileOrPattern string, partitionCount int) *FileSource {

	s := &FileSource{
		PartitionCount: partitionCount,	// 分区数
		FileType:       fileType,		// 文件类型: txt,csv,tsv,zip,orc...
		prefix:         fileType,		// 文件类型:
	}

	// 网络路径
	if strings.Contains(fileOrPattern, "://") {
		u, err := url.Parse(fileOrPattern)
		if err != nil {
			log.Printf("Invalid input URL %s", fileOrPattern)
			return nil
		}
		s.fileBaseName = filepath.Base(u.Path)
		u.Path = filepath.Dir(u.Path)
		s.folder = u.String()
		s.Path = fileOrPattern
	// 本地文件
	} else {
		abs, err := filepath.Abs(fileOrPattern)
		if err != nil {
			log.Fatalf("file \"%s\" not found: %v", fileOrPattern, err)
		}
		s.folder = filepath.Dir(abs)
		s.fileBaseName = filepath.Base(abs)
		s.Path = abs
	}

	if strings.ContainsAny(s.fileBaseName, "*?") {
		s.hasWildcard = true
	}

	// fmt.Printf("file source: %+v\n", s)
	return s
}

func (q *FileSource) genShardInfos(f *flow.Flow) *flow.Dataset {
	return f.Source(
		q.prefix+"."+q.fileBaseName,
		// 构造一个文件分片信息的 row ，并将其写入到 writer 中。
		func(writer io.Writer, stats *pb.InstructionStat) error {
			stats.InputCounter++
			// 如果非正则表达式匹配，且 path 非目录。
			if !q.hasWildcard && !filesystem.IsDir(q.Path) {
				stats.OutputCounter++
				// 构造一个 row 并写入到 writer 中
				util.NewRow(util.Now(), encodeShardInfo(&FileShardInfo{
					FileName:  q.Path,		// 文件名
					FileType:  q.FileType,	// 文件类型
					HasHeader: q.HasHeader,	// 是否包含头部
					Fields:    q.Fields,	// 字段列表
				})).WriteTo(writer)
			} else {
				virtualFiles, err := filesystem.List(q.folder)
				if err != nil {
					return fmt.Errorf("Failed to list folder %s: %v", q.folder, err)
				}
				for _, vf := range virtualFiles {
					if !q.hasWildcard || q.match(vf.Location) {
						stats.OutputCounter++
						util.NewRow(util.Now(), encodeShardInfo(&FileShardInfo{
							FileName:  vf.Location,
							FileType:  q.FileType,
							HasHeader: q.HasHeader,
							Fields:    q.Fields,
						})).WriteTo(writer)
					}
				}
			}
			return nil
		},
	)
}

func (q *FileSource) match(fullPath string) bool {
	baseName := filepath.Base(fullPath)
	match, _ := filepath.Match(q.fileBaseName, baseName)
	return match
}
