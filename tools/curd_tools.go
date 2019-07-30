package main

import (
	"flag"
	"fmt"
	"github.com/DSiSc/blockstore"
	"github.com/DSiSc/blockstore/common"
	"github.com/DSiSc/blockstore/config"
	"os"
)

const latestBlockKey = "LatestBlock"

func main() {
	var blkNums uint64
	var showHelp bool
	var dbPath string
	flagSet := flag.NewFlagSet("db-curd", flag.ExitOnError)
	flagSet.StringVar(&dbPath, "f", "", "The block store file path.")
	flagSet.Uint64Var(&blkNums, "d", 0, "The number of blocks to delete.")
	flagSet.BoolVar(&showHelp, "h", false, "Display help.")
	flagSet.Usage = func() {
		fmt.Println(`Justitia Block Store CURD tool.

Usage:
    Delete the latest [num] blocks:  go run curd_tools.go -f [file path] -d [num]

Examples:
    You can use this tool to delete the block from block store.
		
	Delete the latest 2 blocks from block store.
		go run curd_tools.go -f /var/db/ -d 2
   `)
	}
	flagSet.Parse(os.Args[1:])

	if showHelp {
		flagSet.Usage()
		return
	}

	bconf := &config.BlockStoreConfig{
		PluginName: blockstore.PLUGIN_LEVELDB,
		DataPath:   dbPath,
	}
	bStore, err := blockstore.NewBlockStore(bconf)
	if err != nil {
		fmt.Printf("failed to open block store, as: %v\n", err)
		os.Exit(1)
	}

	cBlock := bStore.GetCurrentBlock()
	if cBlock.Header.Height <= blkNums {
		fmt.Printf("have no enough blocks in block store,")
		os.Exit(1)
	}

	blk, err := bStore.GetBlockByHeight(cBlock.Header.Height - blkNums)
	if err != nil {
		fmt.Printf("can't find a block as the latest block after deleting %d blocks from block store, as: %v\n", blk.Header.Height, err)
		os.Exit(1)
	}

	err = bStore.Put([]byte(latestBlockKey), common.HashToBytes(blk.HeaderHash))
	if err != nil {
		fmt.Printf("failed to update current block info, as: %v\n", err)
		os.Exit(1)
	}
}
