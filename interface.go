package blockstore

import (
	"github.com/DSiSc/blockstore/dbstore"
	"github.com/DSiSc/craft/types"
)

// BlockStoreAPI block-store module public api.
type BlockStoreAPI interface {
	dbstore.DBPutter

	// Get get from db
	Get(key []byte) ([]byte, error)

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

	// GetTransactionByHash get transaction by hash
	GetTransactionByHash(hash types.Hash) (*types.Transaction, types.Hash, uint64, uint64, error)

	// WriteBlock write the block and relative receipts to database. return error if write failed.
	WriteBlockWithReceipts(block *types.Block, receipts []*types.Receipt) error

	// GetReceiptByHash get receipt by relative tx's hash
	GetReceiptByTxHash(txHash types.Hash) (*types.Receipt, types.Hash, uint64, uint64, error)

	// GetReceiptByHash get receipt by relative block's hash
	GetReceiptByBlockHash(txHash types.Hash) []*types.Receipt

	// Delete removes the key from the key-value data store.
	Delete(key []byte) error
}
