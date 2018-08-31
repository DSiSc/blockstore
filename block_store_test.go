package blockstore

import (
	"github.com/DSiSc/blockstore/config"
	"github.com/DSiSc/blockstore/util"
	"github.com/DSiSc/craft/types"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	stateHash = util.HexToHash("0x5a0b54d5dc17e0aadc383d2db43b0a0d3e029c4c")
	blockHash = util.HexToHash("0xb3f9a62087cbe321e798966883cbc445d9b924a9bbf2e010957a537ea2da7f02")
)

type MockBlock struct {
	types.Block
	mockCtl *gomock.Controller
}

// mock block store config
func mockBlockStoreConfig() *config.BlockStoreConfig {
	return &config.BlockStoreConfig{
		PluginName: PLUGIN_MEMDB,
		DataPath:   "./",
	}
}

// mock block
func mockBlock() *types.Block {
	header := types.Header{
		Height:    1,
		StateRoot: stateHash,
		BlockHash: blockHash,
	}
	block := &types.Block{
		Header: &header,
	}
	return block
}

// test create block store
func TestNewBlockStore(t *testing.T) {
	assert := assert.New(t)
	blockStore, err := NewBlockStore(mockBlockStoreConfig())
	assert.Nil(err)
	assert.NotNil(blockStore)
}

// create block store inner database
func TestBlockStore_createDBStore(t *testing.T) {
	assert := assert.New(t)
	config := mockBlockStoreConfig()
	database, err := createDBStore(config)
	assert.Nil(err)
	assert.NotNil(database)
	config.PluginName = PLUGIN_LEVELDB
	database, err = createDBStore(config)
	assert.Nil(err)
	assert.NotNil(database)
}

// test write block
func TestBlockStore_WriteBlock(t *testing.T) {
	assert := assert.New(t)
	blockStore, err := NewBlockStore(mockBlockStoreConfig())
	assert.Nil(err)
	assert.NotNil(blockStore)
	block := mockBlock()
	err = blockStore.WriteBlock(block)
	assert.Nil(err)
}

// test get block by hash
func TestBlockStore_GetBlockByHash(t *testing.T) {
	assert := assert.New(t)
	blockStore, err := NewBlockStore(mockBlockStoreConfig())
	assert.Nil(err)
	assert.NotNil(blockStore)
	block := mockBlock()
	err = blockStore.WriteBlock(block)
	assert.Nil(err)

	blockSaved, err := blockStore.GetBlockByHash(block.Header.BlockHash)
	assert.Nil(err)
	assert.Equal(block.Header.BlockHash, blockSaved.Header.BlockHash)
}

// test get block by height
func TestBlockStore_GetBlockByHeight(t *testing.T) {
	assert := assert.New(t)
	blockStore, err := NewBlockStore(mockBlockStoreConfig())
	assert.Nil(err)
	assert.NotNil(blockStore)
	block := mockBlock()
	err = blockStore.WriteBlock(block)
	assert.Nil(err)

	blockSaved, err := blockStore.GetBlockByHeight(1)
	assert.Nil(err)
	assert.Equal(block.Header.BlockHash, blockSaved.Header.BlockHash)
}

// test get current block
func TestBlockStore_GetCurrentBlock(t *testing.T) {
	assert := assert.New(t)
	blockStore, err := NewBlockStore(mockBlockStoreConfig())
	assert.Nil(err)
	assert.NotNil(blockStore)
	block := mockBlock()
	err = blockStore.WriteBlock(block)
	assert.Nil(err)

	blockCurrent := blockStore.GetCurrentBlock()
	assert.Equal(block.Header.BlockHash, blockCurrent.Header.BlockHash)
}
