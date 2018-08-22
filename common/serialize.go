package common

import (
"encoding/binary"
"io"
)

func WriteUint32(writer io.Writer, val uint32) error {
	var p [4]byte
	binary.LittleEndian.PutUint32(p[:], val)
	_, err := writer.Write(p[:])
	return err
}

func WriteUint64(writer io.Writer, val uint64) error {
	var p [8]byte
	binary.LittleEndian.PutUint64(p[:], val)
	_, err := writer.Write(p[:])
	return err
}
func Serialize(w io.Writer, f *int64) error {
	err := binary.Write(w, binary.LittleEndian, int64(*f))
	return err
}

func WriteVarUint(writer io.Writer, value uint64) error {
	var buf [9]byte
	var len = 0
	if value < 0xFD {
		buf[0] = uint8(value)
		len = 1
	} else if value <= 0xFFFF {
		buf[0] = 0xFD
		binary.LittleEndian.PutUint16(buf[1:], uint16(value))
		len = 3
	} else if value <= 0xFFFFFFFF {
		buf[0] = 0xFE
		binary.LittleEndian.PutUint32(buf[1:], uint32(value))
		len = 5
	} else {
		buf[0] = 0xFF
		binary.LittleEndian.PutUint64(buf[1:], uint64(value))
		len = 9
	}
	_, err := writer.Write(buf[:len])
	return err
}

func WriteVarBytes(writer io.Writer, value []byte) error {
	err := WriteVarUint(writer, uint64(len(value)))
	if err != nil {
		return err
	}
	_, err = writer.Write(value)
	return err
}

