package blockstore

import (
	"github.com/DSiSc/craft/types"
)

// BlockStoreAPI block-store module public api.
type BlockStoreAPI interface {

	// WriteBlock write the block to database. return error if write failed.
	WriteBlock(block *types.Block) error

	// GetBlockByHash get block by block hash.
	GetBlockByHash(hash types.Hash) (*types.Block, error)

	// GetBlockByHeight get block by height.
	GetBlockByHeight(height uint64) (*types.Block, error)

	// GetCurrentBlock get current block.
	GetCurrentBlock() *types.Block

	// GetCurrentBlockHeight get current block height.
	GetCurrentBlockHeight() uint64
}
