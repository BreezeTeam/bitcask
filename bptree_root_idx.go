package bitcask

import (
	"encoding/binary"
	"hash/crc32"
	"os"
)

// BPTreeRootIdxHeaderSize returns the header size of the root index.
const BPTreeRootIdxHeaderSize = 28

// BPTreeRootIdx represents the b+ tree root index.
type BPTreeRootIdx struct {
	crc       uint32
	fID       uint64
	rootOff   uint64
	startSize uint32
	endSize   uint32
	start     []byte
	end       []byte
}

// Encode returns the slice after the BPTreeRootIdx be encoded.
func (bri *BPTreeRootIdx) Encode() []byte {
	buf := make([]byte, bri.Size())
	binary.LittleEndian.PutUint64(buf[4:12], bri.fID)
	binary.LittleEndian.PutUint64(buf[12:20], bri.rootOff)
	binary.LittleEndian.PutUint32(buf[20:24], bri.startSize)
	binary.LittleEndian.PutUint32(buf[24:28], bri.endSize)

	startOff := BPTreeRootIdxHeaderSize
	endOff := startOff + int(bri.startSize)
	copy(buf[startOff:endOff], bri.start)

	startOff = endOff
	endOff = startOff + int(bri.endSize)
	copy(buf[startOff:endOff], bri.end)

	c32 := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], c32)

	return buf
}

// GetCrc returns the crc at given buf slice.
func (bri *BPTreeRootIdx) GetCrc(buf []byte) uint32 {
	crc := crc32.ChecksumIEEE(buf[4:])
	crc = crc32.Update(crc, crc32.IEEETable, bri.start)
	crc = crc32.Update(crc, crc32.IEEETable, bri.end)

	return crc
}

// Size returns the size of the BPTreeRootIdx entry.
func (bri *BPTreeRootIdx) Size() int64 {
	return BPTreeRootIdxHeaderSize + int64(bri.startSize) + int64(bri.endSize)
}

// IsZero checks if the BPTreeRootIdx entry is zero or not.
func (bri *BPTreeRootIdx) IsZero() bool {
	if bri.crc == 0 && bri.rootOff == 0 && bri.fID == 0 && bri.startSize == 0 && bri.endSize == 0 {
		return true
	}
	return false
}

// ReadBPTreeRootIdxAt reads BPTreeRootIdx entry from the File starting at byte offset off.
func ReadBPTreeRootIdxAt(fd *os.File, off int64) (*BPTreeRootIdx, error) {
	buf := make([]byte, BPTreeRootIdxHeaderSize)
	_, err := fd.ReadAt(buf, off)
	if err != nil {
		return nil, err
	}
	bri := &BPTreeRootIdx{}
	bri.fID = binary.LittleEndian.Uint64(buf[4:12])
	bri.rootOff = binary.LittleEndian.Uint64(buf[12:20])
	bri.startSize = binary.LittleEndian.Uint32(buf[20:24])
	bri.endSize = binary.LittleEndian.Uint32(buf[24:28])

	if bri.IsZero() {
		return nil, nil
	}

	off += BPTreeRootIdxHeaderSize
	startBuf := make([]byte, bri.startSize)
	_, err = fd.ReadAt(startBuf, off)
	if err != nil {
		return nil, err
	}
	bri.start = startBuf

	off += int64(bri.startSize)
	endBuf := make([]byte, bri.endSize)
	_, err = fd.ReadAt(endBuf, off)
	if err != nil {
		return nil, err
	}
	bri.end = endBuf

	bri.crc = binary.LittleEndian.Uint32(buf[0:4])
	if bri.GetCrc(buf) != bri.crc {
		return nil, ErrCrc
	}

	return bri, nil
}
