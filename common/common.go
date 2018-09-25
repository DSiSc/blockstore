package common

import (
	"crypto/sha256"
	"encoding/json"
	"github.com/DSiSc/craft/types"
)

type DataEntryPrefix byte

const (
	// DATA
	DATA_BLOCK       DataEntryPrefix = 0x00 //Block height => block hash key prefix
	DATA_HEADER                      = 0x01 //Block hash => block hash key prefix
	DATA_TRANSACTION                 = 0x02 //Transction hash = > transaction key prefix
)

// BlockHash calculate block's hash
func BlockHash(block *types.Block) (hash types.Hash) {
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
