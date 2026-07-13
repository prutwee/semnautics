package types

type OpType uint8 // CDC operation type

const (
	OpInsert  OpType = iota
	OpUpdate  OpType = iota
	OpDelete  OpType = iota
	OpUnknown OpType = iota
)

type Event struct {
	Payload   []byte
	Namespace string
	Timestamp int64
	Operation OpType
}
