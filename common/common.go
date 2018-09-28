package common

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/DSiSc/craft/log"
	"github.com/DSiSc/craft/types"
)

type DataEntryPrefix byte

const (
	// DATA
	DATA_BLOCK       DataEntryPrefix = 0x00 //Block height => block hash key prefix
	DATA_HEADER                      = 0x01 //Block hash => block hash key prefix
	DATA_TRANSACTION                 = 0x02 //Transction hash = > transaction key prefix
	HashLength                       = 32
	AddressLength                    = 20
)

// BytesToAddress returns Address with value b.
// If b is larger than len(h), b will be cropped from the left.
func BytesToAddress(b []byte) types.Address {
	var a types.Address
	if len(b) > len(a) {
		b = b[len(b)-AddressLength:]
	}
	copy(a[AddressLength-len(b):], b)
	return a
}

// HexToAddress returns Address with byte values of s.
// If s is larger than len(h), s will be cropped from the left.
func HexToAddress(s string) types.Address { return BytesToAddress(FromHex(s)) }

// HexToHash sets byte representation of s to hash.
// If b is larger than len(h), b will be cropped from the left.
func HexToHash(s string) types.Hash { return BytesToHash(FromHex(s)) }

// BytesToHash sets b to hash.
// If b is larger than len(h), b will be cropped from the left.
func BytesToHash(b []byte) types.Hash {
	var h types.Hash
	if len(b) > len(h) {
		b = b[len(b)-HashLength:]
	}

	copy(h[HashLength-len(b):], b)
	return h
}

// HashToBytes convert hash to []byte
func HashToBytes(hash types.Hash) []byte {
	return hash[:]
}

// FromHex returns the bytes represented by the hexadecimal string s.
// s may be prefixed with "0x".
func FromHex(s string) []byte {
	if len(s) > 1 {
		if s[0:2] == "0x" || s[0:2] == "0X" {
			s = s[2:]
		}
	}
	if len(s)%2 == 1 {
		s = "0" + s
	}
	return Hex2Bytes(s)
}

// Hex2Bytes returns the bytes represented by the hexadecimal string str.
func Hex2Bytes(str string) []byte {
	h, _ := hex.DecodeString(str)
	return h
}

// Encode encodes b as a hex string with 0x prefix.
func Encode(b []byte) string {
	enc := make([]byte, len(b)*2+2)
	copy(enc, "0x")
	hex.Encode(enc[2:], b)
	return string(enc)
}

// TxHash calculate tx's hash
func TxHash(tx *types.Transaction) (hash types.Hash) {
	if nil == tx {
		log.Warn("Tx is nil, so it's hash meaningless.")
		return
	}
	jsonByte, _ := json.Marshal(tx)
	sumByte := Sum(jsonByte)
	copy(hash[:], sumByte)
	return
}

// HeaderHash calculate block's hash
func HeaderHash(block *types.Block) (hash types.Hash) {
	jsonByte, _ := json.Marshal(block.Header)
	sumByte := Sum(jsonByte)
	copy(hash[:], sumByte)
	return
}

// Sum returns the first 20 bytes of SHA256 of the bz.
func Sum(bz []byte) []byte {
	hash := sha256.Sum256(bz)
	return hash[:types.HashLength]
}
