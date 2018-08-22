package common

type DataEntryPrefix byte

const (
	// DATA
	DATA_BLOCK       DataEntryPrefix = 0x00 //Block height => block hash key prefix
	DATA_HEADER                      = 0x01 //Block hash => block hash key prefix
	DATA_TRANSACTION                 = 0x02 //Transction hash = > transaction key prefix
)
