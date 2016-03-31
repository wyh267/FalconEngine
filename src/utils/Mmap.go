/*****************************************************************************
 *  file name : Mmap.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : mmap底层封装
 *
******************************************************************************/

package utils

import (
	//"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"reflect"
	"syscall"
	"unsafe"
)

type Mmap struct {
	MmapBytes   []byte
	FileName    string
	FileLen     int64
	FilePointer int64
	MapType     int64
	FileFd      *os.File
}

const APPEND_DATA int64 = 1024 * 1024
const (
	MODE_APPEND = iota
	MODE_CREATE
)



func NewMmap(file_name string, mode int) (*Mmap, error) {

	this := &Mmap{MmapBytes: make([]byte, 0), FileName: file_name, FileLen: 0, MapType: 0, FilePointer: 0, FileFd: nil}

	file_mode := os.O_RDWR
	file_create_mode := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	if mode == MODE_CREATE {
		file_mode = os.O_RDWR | os.O_CREATE | os.O_TRUNC
	}

	f, err := os.OpenFile(file_name, file_mode, 0664)

	if err != nil {
		f, err = os.OpenFile(file_name, file_create_mode, 0664)
		if err != nil {
			return nil, err
		}
	}

	fi, err := f.Stat()
	if err != nil {
		fmt.Printf("ERR:%v", err)
	}
	this.FileLen = fi.Size()
	if mode == MODE_CREATE || this.FileLen == 0 {
		syscall.Ftruncate(int(f.Fd()), fi.Size()+APPEND_DATA)
		this.FileLen = APPEND_DATA
	}
	this.MmapBytes, err = syscall.Mmap(int(f.Fd()), 0, int(this.FileLen), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)

	if err != nil {
		fmt.Printf("MAPPING ERROR  %v \n", err)
		return nil, err
	}

	this.FileFd = f
	return this, nil
}

func (this *Mmap) SetFileEnd(file_len int64) {
	this.FilePointer = file_len
}

func (this *Mmap) checkFilePointer(check_value int64) error {

	if this.FilePointer+check_value >= this.FileLen {
		err := syscall.Ftruncate(int(this.FileFd.Fd()), this.FileLen+APPEND_DATA)
		if err != nil {
			fmt.Printf("ftruncate error : %v\n", err)
			return err
		}
		this.FileLen += APPEND_DATA
		syscall.Munmap(this.MmapBytes)
		this.MmapBytes, err = syscall.Mmap(int(this.FileFd.Fd()), 0, int(this.FileLen), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)

		if err != nil {
			fmt.Printf("MAPPING ERROR  %v \n", err)
			return err
		}

	}

	return nil
}

func (this *Mmap) checkFileCap(start, lens int64) error {

	if start+lens >= this.FileLen {
		err := syscall.Ftruncate(int(this.FileFd.Fd()), this.FileLen+APPEND_DATA)
		if err != nil {
			fmt.Printf("ftruncate error : %v\n", err)
			return err
		}

		this.FileLen += APPEND_DATA
		this.FilePointer = start + lens
	}

	return nil

}

func (this *Mmap) isEndOfFile(start int64) bool {

	if this.FilePointer == start {
		return true
	}
	return false

}

func (this *Mmap) ReadInt64(start int64) int64 {

	return int64(binary.LittleEndian.Uint64(this.MmapBytes[start : start+8]))
}

func (this *Mmap) ReadUInt64(start uint64) uint64 {

	return binary.LittleEndian.Uint64(this.MmapBytes[start : start+8])
}

func (this *Mmap) ReadUInt64Arry(start, len uint64) []DocIdNode {

	arry := *(*[]DocIdNode)(unsafe.Pointer(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&this.MmapBytes[start])),
		Len:  int(len),
		Cap:  int(len),
	}))
	return arry
}

func (this *Mmap) ReadDocIdsArry(start, len uint64) []DocIdNode {

	arry := *(*[]DocIdNode)(unsafe.Pointer(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&this.MmapBytes[start])),
		Len:  int(len),
		Cap:  int(len),
	}))
	return arry
}

func (this *Mmap) ReadString(start, lens int64) string {

	return string(this.MmapBytes[start : start+lens])
}

func (this *Mmap) Read(start, end int64) []byte {

	return this.MmapBytes[start:end]
}


func (this *Mmap) Write(start int64,buffer []byte) error {

    copy(this.MmapBytes[start:int(start)+len(buffer)],buffer)

	return nil//this.MmapBytes[start:end]
}



func (this *Mmap) WriteUInt64(start int64, value uint64) error {

	binary.LittleEndian.PutUint64(this.MmapBytes[start:start+8], uint64(value))

	return nil //this.Sync()
}

func (this *Mmap) WriteInt64(start, value int64) error {
	binary.LittleEndian.PutUint64(this.MmapBytes[start:start+8], uint64(value))
	return nil //this.Sync()
}

func (this *Mmap) AppendInt64(value int64) error {

	if err := this.checkFilePointer(8); err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(this.MmapBytes[this.FilePointer:this.FilePointer+8], uint64(value))
	this.FilePointer += 8
	return nil //this.Sync()
}

func (this *Mmap) AppendUInt64(value uint64) error {

	if err := this.checkFilePointer(8); err != nil {
		return err
	}

	binary.LittleEndian.PutUint64(this.MmapBytes[this.FilePointer:this.FilePointer+8], value)
	this.FilePointer += 8
	return nil //this.Sync()
}

func (this *Mmap) AppendStringWithLen(value string) error {
	this.AppendInt64(int64(len(value)))
	this.AppendString(value)
	return nil //this.Sync()

}

func (this *Mmap) AppendString(value string) error {

	lens := int64(len(value))
	if err := this.checkFilePointer(lens); err != nil {
		return err
	}

	dst := this.MmapBytes[this.FilePointer : this.FilePointer+lens]
	copy(dst, []byte(value))
	this.FilePointer += lens
	return nil //this.Sync()

}

func (this *Mmap) AppendBytes(value []byte) error {
	lens := int64(len(value))
	if err := this.checkFilePointer(lens); err != nil {
		return err
	}
	dst := this.MmapBytes[this.FilePointer : this.FilePointer+lens]
	copy(dst, value)
	this.FilePointer += lens
	return nil //this.Sync()

}

func (this *Mmap) WriteBytes(start int64, value []byte) error {
	lens := int64(len(value))
	dst := this.MmapBytes[start : start+lens]
	copy(dst, value)
	return nil //this.Sync()
}

func (this *Mmap) Unmap() error {

	syscall.Munmap(this.MmapBytes)
	this.FileFd.Close()
	return nil
}

func (this *Mmap) GetPointer() int64 {
	return this.FilePointer
}

func (this *Mmap) header() *reflect.SliceHeader {
	return (*reflect.SliceHeader)(unsafe.Pointer(&this.MmapBytes))
}

func (this *Mmap) Sync() error {
	dh := this.header()
	_, _, err := syscall.Syscall(syscall.SYS_MSYNC, dh.Data, uintptr(dh.Len), syscall.MS_SYNC)
	if err != 0 {
		fmt.Printf("Sync Error ")
		return errors.New("Sync Error")
	}
	return nil
}

func (this *Mmap) AppendStringWith32Bytes(value string, lens int64) error {

	err := this.AppendInt64(lens)
	if err != nil {
		return err
	}
	if err := this.checkFilePointer(32); err != nil {
		return err
	}
	dst := this.MmapBytes[this.FilePointer : this.FilePointer+32]
	copy(dst, value)
	this.FilePointer += 32
	return nil //this.Sync()
}

func (this *Mmap) ReadStringWith32Bytes(start int64) string {

	lens := this.ReadInt64(start)
	return this.ReadString(start+8, lens)

}

func (this *Mmap) WriteStringWith32Bytes(start int64, value string, lens int64) error {

	this.WriteInt64(start, lens)
	this.WriteBytes(start+4, []byte(value))
	return nil
}
