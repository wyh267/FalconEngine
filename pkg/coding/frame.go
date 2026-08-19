// Package coding 提供带 CRC 校验的记录帧编码与常用变长编码工具。
// 帧格式：[4B CRC32(payload)][8B 长度][payload]
package coding

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/FalconEngine/falcon/types"
)

// FrameHeaderSize 帧头长度：4B CRC + 8B 长度
const FrameHeaderSize = 12

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// WriteFrame 写入一个带 CRC 校验的记录帧，返回帧起始偏移
func WriteFrame(w types.Writer, payload []byte) (int64, error) {
	off := w.Offset()
	var hdr [FrameHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], crc32.Checksum(payload, castagnoli))
	binary.LittleEndian.PutUint64(hdr[4:12], uint64(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return 0, err
	}
	if _, err := w.Write(payload); err != nil {
		return 0, err
	}
	return off, nil
}

// ReadFrame 从 offset 处读取一个记录帧并校验 CRC，返回 payload 与下一帧偏移
func ReadFrame(r types.RandomReader, offset int64) ([]byte, int64, error) {
	hdr, err := r.ReadBytes(offset, FrameHeaderSize)
	if err != nil {
		return nil, offset, err
	}
	crc := binary.LittleEndian.Uint32(hdr[0:4])
	length := binary.LittleEndian.Uint64(hdr[4:12])
	payload, err := r.ReadBytes(offset+FrameHeaderSize, int64(length))
	if err != nil {
		return nil, offset, err
	}
	if crc32.Checksum(payload, castagnoli) != crc {
		return nil, offset, fmt.Errorf("frame crc mismatch at offset %d", offset)
	}
	return payload, offset + FrameHeaderSize + int64(length), nil
}

// EncodeUint64Slice 将 uint64 切片做 delta + uvarint 编码
func EncodeUint64Slice(vals []uint64) []byte {
	out := make([]byte, 0, len(vals)*2)
	var buf [binary.MaxVarintLen64]byte
	prev := uint64(0)
	for i, v := range vals {
		delta := v
		if i > 0 {
			delta = v - prev
		}
		prev = v
		n := binary.PutUvarint(buf[:], delta)
		out = append(out, buf[:n]...)
	}
	return out
}

// DecodeUint64Slice 解码 delta + uvarint 编码的 uint64 切片
func DecodeUint64Slice(b []byte, count int) ([]uint64, error) {
	vals := make([]uint64, 0, count)
	prev := uint64(0)
	for len(b) > 0 && len(vals) < count {
		delta, n := binary.Uvarint(b)
		if n <= 0 {
			return nil, fmt.Errorf("bad uvarint in slice decode")
		}
		prev += delta
		vals = append(vals, prev)
		b = b[n:]
	}
	if len(vals) != count {
		return nil, fmt.Errorf("decode count mismatch: want %d got %d", count, len(vals))
	}
	return vals, nil
}
