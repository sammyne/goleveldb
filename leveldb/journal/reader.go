// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmair.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Package journal allows read and write sequence of data block.
package journal

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/syndtr/goleveldb/leveldb/hash"
)

type DropFunc func(n int, reason string)

// Reader represent a journal reader.
type Reader struct {
	r        io.ReaderAt
	checksum bool
	dropf    DropFunc

	eof       bool
	rbuf, buf []byte
	off       int
	record    []byte
	err       error
}

// NewReader create new initialized journal reader.
func NewReader(r io.ReaderAt, checksum bool, dropf DropFunc) *Reader {
	return &Reader{
		r:        r,
		checksum: checksum,
		dropf:    dropf,
	}
}

// Skip allow skip given number bytes, rounded by single block.
func (r *Reader) Skip(skip int) error {
	if skip > 0 {
		r.off = skip / BlockSize
		if skip%BlockSize > 0 {
			r.off++
		}
	}
	return nil
}

func (r *Reader) drop(n int, reason string) {
	if r.dropf != nil {
		r.dropf(n, reason)
	}
}

// Next read the next return, return true if there is next record,
// otherwise return false.
func (r *Reader) Next() bool {
	if r.err != nil {
		return false
	}

	r.record = nil

	inFragment := false
	buf := new(bytes.Buffer)
	for {
		var rec []byte
		var rtype uint
		rec, rtype, r.err = r.read()
		if r.err != nil {
			return false
		}

		switch rtype {
		case tFull:
			if inFragment {
				r.drop(buf.Len(), "partial record without end; tag=full")
				buf.Reset()
			}
			buf.Write(rec)
			r.record = buf.Bytes()
			return true
		case tFirst:
			if inFragment {
				r.drop(buf.Len(), "partial record without end; tag=first")
				buf.Reset()
			}
			buf.Write(rec)
			inFragment = true
		case tMiddle:
			if inFragment {
				buf.Write(rec)
			} else {
				r.drop(len(rec), "missing start of fragmented record; tag=mid")
			}
		case tLast:
			if inFragment {
				buf.Write(rec)
				r.record = buf.Bytes()
				return true
			} else {
				r.drop(len(rec), "missing start of fragmented record; tag=last")
			}
		case tEof:
			if inFragment {
				r.drop(buf.Len(), "partial record without end; tag=eof")
			}
			return false
		case tCorrupt:
			if inFragment {
				r.drop(buf.Len(), "record fragment corrupted")
				buf.Reset()
				inFragment = false
			}
		}
	}
	return false
}

// Record return current record.
func (r *Reader) Record() []byte {
	return r.record
}

// Error return any record produced by previous operation.
func (r *Reader) Error() error {
	return r.err
}

func (r *Reader) read() (ret []byte, rtype uint, err error) {
retry:
	if len(r.buf) < kHeaderSize {
		if r.eof {
			if len(r.buf) > 0 {
				r.drop(len(r.buf), "truncated record at end of file")
				r.rbuf = nil
				r.buf = nil
			}
			rtype = tEof
			return
		}

		if r.rbuf == nil {
			r.rbuf = make([]byte, BlockSize)
		} else {
			r.off++
		}

		var n int
		n, err = r.r.ReadAt(r.rbuf, int64(r.off)*BlockSize)
		if err != nil {
			if err == io.EOF {
				err = nil
			} else {
				return
			}
		}
		r.buf = r.rbuf[:n]
		if n < BlockSize {
			r.eof = true
			goto retry
		}
	}

	// decode record length and type
	recLen := int(r.buf[4]) | (int(r.buf[5]) << 8)
	rtype = uint(r.buf[6])

	// check whether the header is sane
	if len(r.buf) < kHeaderSize+recLen || rtype > tLast {
		rtype = tCorrupt
		r.drop(len(r.buf), "header corrupted")
	} else if r.checksum {
		// decode the checksum
		recCrc := hash.UnmaskCRC32(binary.LittleEndian.Uint32(r.buf))
		crc := hash.NewCRC32C()
		crc.Write(r.buf[6 : kHeaderSize+recLen])
		if crc.Sum32() != recCrc {
			// Drop the rest of the buffer since "length" itself may have
			// been corrupted and if we trust it, we could find some
			// fragment of a real journal record that just happens to look
			// like a valid journal record.
			rtype = tCorrupt
			r.drop(len(r.buf), "checksum mismatch")
		}
	}

	if rtype == tCorrupt {
		// report bytes drop
		r.buf = nil
	} else {
		ret = r.buf[kHeaderSize : kHeaderSize+recLen]
		r.buf = r.buf[kHeaderSize+recLen:]
	}

	return
}
