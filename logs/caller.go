package logs

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"strings"
	"time"
)

type Caller struct {
	File string `json:"file"`
	Line int    `json:"line"`
	Hash string `json:"hash"`
}

type CallerContext []Caller

func (c CallerContext) Append(file string, line int) CallerContext {
	return append(c, Caller{
		File: file,
		Line: line,
		Hash: newHash(),
	})
}

func (c CallerContext) String() string {
	var b strings.Builder
	for _, caller := range c {
		if b.Len() > 0 {
			b.WriteString("->")
		}
		fmt.Fprintf(&b, "%s:%d", caller.File, caller.Line)
		b.WriteRune('(')
		b.WriteString(caller.Hash)
		b.WriteRune(')')
	}
	return b.String()
}

func (c CallerContext) ShortString() string {
	var b strings.Builder
	for _, caller := range c {
		if b.Len() > 0 {
			b.WriteString("->")
		}
		b.WriteString(caller.Hash)
	}
	return b.String()
}

func newHash() string {
	return hex.EncodeToString(
		binary.BigEndian.AppendUint32([]byte{},
			crc32.ChecksumIEEE(
				time.Now().AppendFormat(
					make([]byte, 0, len(time.StampNano)),
					time.StampNano,
				),
			),
		),
	)
}
