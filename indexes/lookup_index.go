package indexes

import "github.com/DSiSc/craft/types"

// EntityLookupIndex is a positional metadata to help looking up the data content of
// a transaction or receipt given only its hash.
type EntityLookupIndex struct {
	BlockHash types.Hash
	Index     uint64
}
