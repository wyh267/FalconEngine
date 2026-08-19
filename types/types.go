// Package types 定义 FalconEngine 最底层的基础类型与编解码接口。
// 本包不依赖任何业务包，处于依赖链最底层。
package types

import "io"

// Writer 顺序写入接口（只允许追加写）
type Writer interface {
	io.WriteCloser
	// WriteBytes 写入字节流，返回写入起始偏移
	WriteBytes(b []byte) (int64, error)
	// WriteUint64 写入定长 8 字节无符号整数
	WriteUint64(v uint64) error
	// WriteUvarint 写入变长无符号整数
	WriteUvarint(v uint64) error
	// Offset 返回当前写入位置
	Offset() int64
	// Sync 强制刷盘
	Sync() error
}

// RandomReader 随机读接口（mmap / 内存后端共用）
type RandomReader interface {
	// ReadBytes 读取 [offset, offset+length) 的数据。
	// 返回的切片可能是底层 mmap 的零拷贝子切片，调用方不得修改。
	ReadBytes(offset, length int64) ([]byte, error)
	// ReadUint64 从 offset 处读取定长 8 字节无符号整数
	ReadUint64(offset int64) (uint64, error)
	// ReadUvarint 从 offset 处读取变长无符号整数，返回新偏移
	ReadUvarint(offset int64) (uint64, int64, error)
	// Len 返回数据总长度
	Len() int64
	io.Closer
}

// Encoder 可编码为字节流
type Encoder interface {
	Encode() ([]byte, error)
}

// Decoder 从字节流解码
type Decoder interface {
	Decode(b []byte) error
}
