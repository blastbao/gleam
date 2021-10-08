package resource

import (
	"io"
	"os"
	"path/filepath"

	"github.com/OneOfOne/xxhash"
)

type FileResource struct {
	FullPath     string `json:"path,omitempty"`
	TargetFolder string `json:"targetFolder,omitempty"`
}

type FileHash struct {
	FullPath     string `json:"path,omitempty"`				// 文件路径
	TargetFolder string `json:"targetFolder,omitempty"`		//
	File         string `json:"file,omitempty"`				// 文件名
	Hash         uint32 `json:"hash,omitempty"` 			// 文件内容哈希值
}

func GenerateFileHash(fullpath string) (*FileHash, error) {
	// 检查文件是否存在，不存在报错
	if _, err := os.Stat(fullpath); os.IsNotExist(err) {
		return nil, err
	}

	// 打开文件
	f, err := os.Open(fullpath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// 计算文件内容的 Hash 值
	hasher := xxhash.New32()
	if _, err := io.Copy(hasher, f); err != nil {
		return nil, err
	}
	crc := hasher.Sum32()

	// 返回值
	return &FileHash{
		FullPath: fullpath,						// 文件路径
		File:     filepath.Base(fullpath),		// 文件名
		Hash:     crc,							// 文件内容哈希值
	}, nil
}
