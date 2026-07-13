package types

type MappedRecord struct {
	DimensionHash uint64  // DimensionHash is a deterministic hash of the grouping key (hash("name:vivian"))
	Value         float64 // later we can support interfaces or exact num types
	Operation     OpType
}
