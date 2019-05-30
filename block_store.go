package blockstore

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/DSiSc/blockstore/common"
	"github.com/DSiSc/blockstore/config"
	"github.com/DSiSc/blockstore/dbstore"
	"github.com/DSiSc/blockstore/dbstore/leveldbstore"
	"github.com/DSiSc/blockstore/dbstore/memorystore"
	"github.com/DSiSc/blockstore/indexes"
	"github.com/DSiSc/craft/log"
	"github.com/DSiSc/craft/types"
	"sync"
	"sync/atomic"
)

const (
	// DB plugin
	PLUGIN_LEVELDB = "leveldb"
	// memory plugin
	PLUGIN_MEMDB = "memorydb"
	// block height before genesis block
	INIT_BLOCK_HEIGHT = 0
	// latestBlockKey tracks the latest know full block's hash.
	latestBlockKey = "LatestBlock"
)

// The fields below define the low level database schema prefixing.
var (
	blockPrefix       = []byte("b")
	blockHeightPrefix = []byte("h")
	txPrefix          = []byte("t")
	receiptPrefix     = []byte("r")
)

// Block store save the data of block & transaction
type BlockStore struct {
	store        dbstore.DBStore // Block store handler
	currentBlock atomic.Value    //Current block
	lock         sync.RWMutex
}

// NewBlockStore return the block store instance
func NewBlockStore(config *config.BlockStoreConfig) (*BlockStore, error) {
	log.Info("Start creating block store, with config: %v ", config)
	store, err := createDBStore(config)
	if err != nil {
		return nil, err
	}
	blockStore := &BlockStore{
		store: store,
	}

	//load latest block from database.
	blockStore.loadLatestBlock()
	return blockStore, nil
}

// init db store.
func createDBStore(config *config.BlockStoreConfig) (dbstore.DBStore, error) {
	switch config.PluginName {
	case PLUGIN_LEVELDB:
		log.Debug("Create file-based block store, with file path: %s ", config.DataPath)
		return leveldbstore.NewLevelDBStore(config.DataPath)
	case PLUGIN_MEMDB:
		log.Debug("Create memory-based block store")
		return memorystore.NewMemDBStore(), nil
	default:
		log.Error("Not support plugin.")
		return nil, fmt.Errorf("Not support plugin type %s", config.PluginName)
	}
}

// load latest block from database.
func (blockStore *BlockStore) loadLatestBlock() {
	log.Info("Start loading block from database")
	blockHashByte, err := blockStore.store.Get([]byte(latestBlockKey))
	if err != nil {
		log.Warn("Failed to load latest block hash from database, we will set current block to nil")
		return
	}

	// load latest block by hash
	blockHash := common.BytesToHash(blockHashByte)
	latestBlock, err := blockStore.GetBlockByHash(blockHash)
	if err != nil {
		log.Warn("Failed to load the latest block with the hash of the record in the database, we will set current block to nil")
		return
	}
	blockStore.currentBlock.Store(latestBlock)
}

// WriteBlock write the block to database. return error if write failed.
func (blockStore *BlockStore) WriteBlock(block *types.Block) error {
	batch := blockStore.store.NewBatch()
	err := blockStore.writeBlockByBatch(batch, block)
	if err != nil {
		batch.Reset()
		return err
	}
	err = batch.Write()
	if err != nil {
		log.Error("failed to commit block %x to database, as: %v", block.HeaderHash, err)
		return err
	}

	// update current block
	blockStore.recordCurrentBlock(block)
	return nil
}

// WriteBlock write the block to database. return error if write failed.
func (blockStore *BlockStore) writeBlockByBatch(batch dbstore.Batch, block *types.Block) error {
	// write block
	log.Info("Start writing block %x to database.", block.HeaderHash)
	blockByte, err := encodeEntity(block)
	if err != nil {
		log.Error("Failed to encode block %v to byte, as: %v ", block, err)
		return fmt.Errorf("Failed to encode block %v to byte, as: %v ", block, err)
	}

	blockHash := common.HeaderHash(block)
	if !bytes.Equal(blockHash[:], block.HeaderHash[:]) {
		log.Error("Invalid block, as block's hash %x is not same to expected %x ", blockHash, block.HeaderHash)
		return fmt.Errorf("Invalid block, as block's hash %x is not same to expected %x ", blockHash, block.HeaderHash)
	}
	err = batch.Put(append(blockPrefix, common.HashToBytes(blockHash)...), blockByte)
	if err != nil {
		log.Error("Failed to write block %x to database, as: %v ", blockHash, err)
		return fmt.Errorf("Failed to write block %x to database, as: %v ", blockHash, err)
	}

	// write block height and hash mapping
	err = batch.Put(append(blockHeightPrefix, encodeBlockHeight(block.Header.Height)...), common.HashToBytes(blockHash))
	if err != nil {
		log.Error("Failed to record the mapping between block and height")
		return fmt.Errorf("Failed to record the mapping between block and height ")
	}

	// write tx lookup index
	err = blockStore.writeTxLookUpIndex(batch, blockHash, block.Header.Height, block.Transactions)
	if err != nil {
		log.Error("Failed to record the tx lookup index from block %x", blockHash)
		return fmt.Errorf("Failed to record the tx lookup index from block %x ", blockHash)
	}

	// update latest block
	err = batch.Put([]byte(latestBlockKey), common.HashToBytes(blockHash))
	if err != nil {
		log.Warn("Failed to record latest block, as: %v. we will still use the previous latest block as current latest block ", err)
	}
	return nil
}

// WriteBlock write the block and relative receipts to database. return error if write failed.
func (blockStore *BlockStore) WriteBlockWithReceipts(block *types.Block, receipts []*types.Receipt) error {
	batch := blockStore.store.NewBatch()
	receiptsByte, err := encodeEntity(receipts)
	if err != nil {
		log.Error("Failed to encode receipts %v to byte, as: %v ", receipts, err)
		return fmt.Errorf("Failed to encode receipts %v to byte, as: %v ", receipts, err)
	}
	blockHash := common.HeaderHash(block)
	batch.Put(append(receiptPrefix, common.HashToBytes(blockHash)...), receiptsByte)
	blockStore.writeBlockByBatch(batch, block)
	if err != nil {
		batch.Reset()
		return err
	}
	err = batch.Write()
	if err != nil {
		log.Error("failed to commit block %x to database, as: %v", block.HeaderHash, err)
		return err
	}

	// update current block
	blockStore.recordCurrentBlock(block)
	return nil
}

// GetBlockByHash get block by block hash.
func (blockStore *BlockStore) GetBlockByHash(hash types.Hash) (*types.Block, error) {
	blockByte, err := blockStore.store.Get(append(blockPrefix, common.HashToBytes(hash)...))
	if blockByte == nil || err != nil {
		return nil, fmt.Errorf("failed to get block with hash %x, as: %v", hash, err)
	}
	var block types.Block
	err = decodeEntity(blockByte, &block)
	if err != nil {
		return nil, fmt.Errorf("failed to decode block with hash %x from database as: %v", hash, err)
	}
	return &block, nil
}

// GetBlockByHeight get block by height.
func (blockStore *BlockStore) GetBlockByHeight(height uint64) (*types.Block, error) {
	blockHashByte, err := blockStore.store.Get(append(blockHeightPrefix, encodeBlockHeight(height)...))
	if err != nil {
		return nil, fmt.Errorf("failed to get block with height %d, as: %v", height, err)
	}
	blockHash := common.BytesToHash(blockHashByte)
	return blockStore.GetBlockByHash(blockHash)
}

// GetCurrentBlock get current block.
func (blockStore *BlockStore) GetCurrentBlock() *types.Block {
	currentBlock := blockStore.currentBlock.Load()
	if currentBlock != nil {
		return currentBlock.(*types.Block)
	} else {
		return nil
	}
}

// GetCurrentBlockHeight get current block height.
func (blockStore *BlockStore) GetCurrentBlockHeight() uint64 {
	currentBlock := blockStore.GetCurrentBlock()
	if currentBlock != nil {
		return currentBlock.Header.Height
	} else {
		return INIT_BLOCK_HEIGHT
	}
}

// GetTransactionByHash get transaction by hash
func (blockStore *BlockStore) GetTransactionByHash(hash types.Hash) (*types.Transaction, types.Hash, uint64, uint64, error) {
	// read tx look up indexs
	txLookupIntex, err := blockStore.getEntityLookUpIndex(hash)
	if err != nil {
		log.Error("failed to decode tx lookup index with hash %x from database as: %v", hash, err)
		return nil, types.Hash{}, 0, 0, fmt.Errorf("failed to decode tx lookup index with hash %x from database as: %v", hash, err)
	}

	// read block include this tx
	block, err := blockStore.GetBlockByHash(txLookupIntex.BlockHash)
	if err != nil {
		return nil, types.Hash{}, 0, 0, err
	}
	return block.Transactions[txLookupIntex.Index], txLookupIntex.BlockHash, txLookupIntex.BlockHeight, txLookupIntex.Index, nil
}

// GetReceiptByHash get receipt by relative tx's hash
func (blockStore *BlockStore) GetReceiptByTxHash(txHash types.Hash) (*types.Receipt, types.Hash, uint64, uint64, error) {
	// read tx look up indexs
	txLookupIntex, err := blockStore.getEntityLookUpIndex(txHash)
	if err != nil {
		log.Error("failed to get tx lookup index with hash %x from database as: %v", txHash, err)
		return nil, types.Hash{}, 0, 0, fmt.Errorf("failed get tx lookup index with hash %x from database as: %v", txHash, err)
	}
	receiptsByte, err := blockStore.store.Get(append(receiptPrefix, common.HashToBytes(txLookupIntex.BlockHash)...))
	if err != nil {
		log.Error("failed to get receipts with block hash %x from database as: %v", txLookupIntex.BlockHash, err)
		return nil, types.Hash{}, 0, 0, fmt.Errorf("failed to get receipts with block hash %x from database as: %v", txLookupIntex.BlockHash, err)
	}
	var receipts []*types.Receipt
	err = decodeEntity(receiptsByte, &receipts)
	if err != nil {
		log.Error("failed to decode receipts with block hash %x, s: %v", txLookupIntex.BlockHash, err)
		return nil, types.Hash{}, 0, 0, fmt.Errorf("failed to decode receipts with block hash %x, s: %v", txLookupIntex.BlockHash, err)
	}
	return receipts[txLookupIntex.Index], txLookupIntex.BlockHash, txLookupIntex.BlockHeight, txLookupIntex.Index, nil
}

// GetReceiptByHash get receipt by relative block's hash
func (blockStore *BlockStore) GetReceiptByBlockHash(blockHash types.Hash) []*types.Receipt {
	receiptsByte, err := blockStore.store.Get(append(receiptPrefix, common.HashToBytes(blockHash)...))
	if err != nil {
		log.Error("failed to get receipts with block hash %x from database as: %v", blockHash, err)
		return nil
	}
	var receipts []*types.Receipt
	err = decodeEntity(receiptsByte, &receipts)
	if err != nil {
		log.Error("failed to decode receipts with block hash %x, s: %v", blockHash, err)
		return nil
	}
	return receipts
}

// Put add a record to database
func (blockStore *BlockStore) Put(key []byte, value []byte) error {
	return blockStore.store.Put(key, value)
}

// Get get a record by key
func (blockStore *BlockStore) Get(key []byte) ([]byte, error) {
	return blockStore.store.Get(key)
}

// Delete removes the key from the key-value data store.
func (blockStore *BlockStore) Delete(key []byte) error {
	return blockStore.store.Delete(key)
}

// getEntityLookUpIndex get look up index entity by hash
func (blockStore *BlockStore) getEntityLookUpIndex(txHash types.Hash) (*indexes.EntityLookupIndex, error) {
	// read tx look up indexs
	txLookupIntexByte, err := blockStore.store.Get(append(txPrefix, common.HashToBytes(txHash)...))
	if txLookupIntexByte == nil || err != nil {
		return nil, fmt.Errorf("failed to get tx lookup index with hash %x, as: %v", txHash, err)
	}
	var txLookupIntex indexes.EntityLookupIndex
	err = decodeEntity(txLookupIntexByte, &txLookupIntex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode tx lookup index with hash %x from database as: %v", txHash, err)
	}
	return &txLookupIntex, nil
}

// stores a positional metadata for every transaction from a block
func (blockStore *BlockStore) writeTxLookUpIndex(batch dbstore.Batch, blockHash types.Hash, blockHeight uint64, txs []*types.Transaction) error {
	for i, tx := range txs {
		index := indexes.EntityLookupIndex{
			BlockHash:   blockHash,
			BlockHeight: blockHeight,
			Index:       uint64(i),
		}
		indexByte, err := encodeEntity(index)
		if err != nil {
			log.Error("Failed to encode tx lookup index %d to byte, as: %v ", index, err)
			return fmt.Errorf("Failed to tx lookup index %d to byte, as: %v ", index, err)
		}
		err = batch.Put(append(txPrefix, common.HashToBytes(common.TxHash(tx))...), indexByte)
		if err != nil {
			log.Error("Failed to store tx lookup index %d to database, as: %v ", index, err)
			return fmt.Errorf("Failed to store tx lookup index %d to database, as: %v ", index, err)
		}
		//check whether the batch size over the max batch size
		if batch.ValueSize() >= dbstore.MaxBatchSize {
			batch.Write()
		}
	}
	return nil
}

// record current block
func (blockStore *BlockStore) recordCurrentBlock(block *types.Block) {
	log.Info("Update current block to %x", block.HeaderHash)
	blockStore.currentBlock.Store(block)
}

// encodeBlockHeight encodes a block height to byte
func encodeBlockHeight(height uint64) []byte {
	enc := make([]byte, 8)
	binary.BigEndian.PutUint64(enc, height)
	return enc
}

// encode entity to byte
func encodeEntity(block interface{}) ([]byte, error) {
	return json.Marshal(block)
}

// decode block from byte
func decodeEntity(blockByte []byte, entityType interface{}) error {
	return json.Unmarshal(blockByte, entityType)
}
