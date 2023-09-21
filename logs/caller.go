package logs

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"

	logsv1 "github.com/daichitakahashi/rsmap/internal/proto/logs/v1"
)

type CallerContext []*logsv1.Caller

func (c CallerContext) Append(file string, line int) CallerContext {
	return append(c, &logsv1.Caller{
		File: file,
		Line: int64(line),
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
		binary.BigEndian.AppendUint32([]byte{}, rand.Uint32()),
	)
}
