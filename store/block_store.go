package store

import (
	"bytes"
	"fmt"
	"github.com/DSiSc/craft/types"
	"github.com/DSiSc/ledger/common"
	"github.com/DSiSc/ledger/store/leveldbstore"
	"github.com/DSiSc/txpool/common/log"
)

const (
	// DB plugin
	PLUGIN_LEVELDB = "leveldb"
)

type DBStore interface {
	Put(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	BatchPut(key []byte, value []byte)
}

// Block store save the data of block & transaction
type BlockStore struct {
	dbDir string  // The path of store file
	store DBStore // Block store handler
}

// NewBlockStore return the block store instance
func NewBlockStore(policy string, dbDir string) (*BlockStore, error) {
	var store DBStore
	var err error

	switch policy {
	case PLUGIN_LEVELDB:
		store, err = leveldbstore.NewLevelDBStore(dbDir)
		if err != nil {
			return nil, err
		}
	default:
		log.Error("Not support plugin.")
		return nil, fmt.Errorf("Not support plugin type %s", policy)
	}

	blockStore := &BlockStore{
		dbDir: dbDir,
		store: store,
	}
	return blockStore, nil
}

//SaveTransaction persist transaction to store
func (this *BlockStore) SaveTransaction(tx *types.Transaction, height uint32) error {
	return this.putTransaction(tx, height)
}

func (this *BlockStore) putTransaction(tx *types.Transaction, height uint32) error {
	txHash := tx.Hash()
	key := this.getTransactionKey(txHash)
	value := bytes.NewBuffer(nil)
	common.WriteUint32(value, height)
	this.store.BatchPut(key, value.Bytes())
	return nil
}

func (this *BlockStore) getTransactionKey(txHash types.Hash) []byte {
	key := bytes.NewBuffer(nil)
	key.WriteByte(byte(common.DATA_TRANSACTION))
	txHash.Serialize(key)
	return key.Bytes()
}

//SaveHeader persist block header to store
func (this *BlockStore) SaveHeader(block *types.Block, sysFee int64) error {
	blockHash := block.Hash()
	key := this.getHeaderKey(blockHash)
	value := bytes.NewBuffer(nil)
	err := common.Serialize(value, &sysFee)
	if err != nil {
		return err
	}
	block.Header.Serialize(value)
	common.WriteUint32(value, uint32(len(block.Transactions)))
	for _, tx := range block.Transactions {
		txHash := tx.Hash()
		err = txHash.Serialize(value)
		if err != nil {
			return err
		}
	}
	this.store.BatchPut(key, value.Bytes())
	return nil
}

func (this *BlockStore) getHeaderKey(blockHash types.Hash) []byte {
	data := blockHash.ToArray()
	key := make([]byte, 1+len(data))
	key[0] = byte(common.DATA_HEADER)
	copy(key[1:], data)
	return key
}

// SaveBlock persist block to store
func (this *BlockStore) SaveBlock(block *types.Block) error {
	blockHeight := block.Header.Height
	err := this.SaveHeader(block, 0)
	if err != nil {
		log.Error("Save hender failed.")
		return fmt.Errorf("SaveHeader error %s", err)
	}
	for _, tx := range block.Transactions {
		err = this.SaveTransaction(tx, blockHeight)
		if err != nil {
			txHash := tx.Hash()
			log.Error("Save transaction failed.")
			return fmt.Errorf("SaveTransaction block height %d tx %s err %s", blockHeight, txHash, err)
		}
	}
	return nil
}
