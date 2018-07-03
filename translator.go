package pilosa

type Translator interface {
	DecodeCreateShard(buf []byte) (err error, index string, shard uint64)
	DecodeCreateIndex(buf []byte) (err error, index string)
	DecodeDeleteIndex(buf []byte) (err error, index string)
	DecodeCreateField(buf []byte) (err error, index, field string, opt FieldOptions)
	DecodeDeleteField(buf []byte) (err error, index, field string)
	DecodeCreateView(buf []byte) (err error, index, field, view string)
	DecodeDeleteView(buf []byte) (err error, index, field, view string)
	DecodeClusterStatus(buf []byte) (err error, clusterID, state string, nodes []*Node)
	DecodeResizeInstruction(buf []byte) (
		err error,
		clusterID, state string,
		nodes []*Node,
		jobID int64,
		node *Node,
		schema *Schema,
		sources []ResizeSource,
		coordinator *Node)
	DecodeResizeInstructionComplete(buf []byte) (err error, jobID uint64, node *Node)
	DecodeEvent(buf []byte) (err error, node *Node, event int)
	DecodeSetCoordinator(buf []byte) (err error, node *Node)
	DecodeUpdateCoordinator(buf []byte) (err error, node *Node)
	DecodeNodeState(buf []byte) (err error, nodeID, nodeState string)
}
