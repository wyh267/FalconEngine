package coding

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/FalconEngine/falcon/storage"
)

// TestFrameRoundTrip 帧写入→读取回环
func TestFrameRoundTrip(t *testing.T) {
	ms := storage.NewMemoryStore()
	payloads := [][]byte{
		[]byte("hello falcon"),
		[]byte(""),
		bytes.Repeat([]byte("x"), 4096),
	}
	offsets := make([]int64, 0, len(payloads))
	for _, p := range payloads {
		off, err := WriteFrame(ms, p)
		if err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
		offsets = append(offsets, off)
	}
	for i, off := range offsets {
		got, next, err := ReadFrame(ms, off)
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		if !bytes.Equal(got, payloads[i]) {
			t.Fatalf("frame %d mismatch", i)
		}
		if i+1 < len(offsets) && next != offsets[i+1] {
			t.Fatalf("frame %d next offset wrong: %d != %d", i, next, offsets[i+1])
		}
	}
}

// TestFrameCRCDetect 篡改 payload 后 CRC 必须报错
func TestFrameCRCDetect(t *testing.T) {
	ms := storage.NewMemoryStore()
	off, _ := WriteFrame(ms, []byte("original data"))
	b, _ := ms.ReadBytes(off+FrameHeaderSize, 1)
	_ = b
	// 直接改底层 buffer
	ms.ReadBytes(off+FrameHeaderSize, 1)
	raw, _ := ms.ReadBytes(0, ms.Len())
	raw[off+FrameHeaderSize] ^= 0xff
	if _, _, err := ReadFrame(ms, off); err == nil {
		t.Fatal("expect crc error")
	}
}

// TestUint64SliceRoundTrip delta+uvarint 切片编解码回环
func TestUint64SliceRoundTrip(t *testing.T) {
	cases := [][]uint64{
		{},
		{0},
		{1, 2, 3, 100, 1000, 65535, 1 << 40},
		{5, 5, 5, 5},
	}
	for _, c := range cases {
		enc := EncodeUint64Slice(c)
		dec, err := DecodeUint64Slice(enc, len(c))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(c) == 0 && len(dec) == 0 {
			continue
		}
		if !reflect.DeepEqual(c, dec) {
			t.Fatalf("mismatch: %v != %v", c, dec)
		}
	}
}
