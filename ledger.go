package ledger

import (
	"fmt"
	"github.com/DSiSc/ledger/config"
	"github.com/DSiSc/ledger/store"
	types "github.com/DSiSc/txpool/common"
	"github.com/DSiSc/txpool/common/log"
	"sync"
)

type Ledger struct {
	BlockStore      *store.BlockStore     //BlockStore for saving block & transaction data
	currBlockHeight uint32                //Current block height
	currBlockHash   types.Hash            //Current block hash
	headerIndex     map[uint32]types.Hash //Header index, Mapping header height => block hash
	savingBlock     bool                  //is saving block now
	lock            sync.RWMutex
}

// NewLedger return Ledger instance
func NewLedger(config config.LedgerStoreConf) (*Ledger, error) {
	ledger := &Ledger{
		headerIndex: make(map[uint32]types.Hash),
	}
	storeType := config.PolicyName
	dataDir := config.DataPath
	blockStore, err := store.NewBlockStore(storeType, dataDir)
	if err != nil {
		log.Error("Create a block store failed.")
		return nil, fmt.Errorf("NewBlockStore error %s", err)
	}
	ledger.BlockStore = blockStore

	return ledger, nil
}
