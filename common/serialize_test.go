package common

import (
	"bytes"
	"testing"
)

func TestReadWriteInt(t *testing.T) {
	b1 := new(bytes.Buffer)
	b2 := new(bytes.Buffer)
	b3 := new(bytes.Buffer)
	b4 := new(bytes.Buffer)

	a1 := uint32(4294967295)
	a2 := uint64(18446744073709551615)
	a3 := []byte{10, 11, 12}

	WriteUint32(b1, a1)
	WriteUint64(b2, a2)
	WriteVarUint(b3, a2)
	WriteVarBytes(b4, a3)
}
