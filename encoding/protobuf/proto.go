package protobuf

import (
	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa"
	"github.com/pkg/errors"
)

//go:generate protoc --gofast_out=. private.proto
//go:generate protoc --gofast_out=. public.proto

type pbTranslator struct{}

func (pb *pbTranslator) DecodeCreateShard(buf []byte) (err error, index string, shard uint64) {
	//if buf[0] == messageTypeCreateShard:
	m := &CreateShardMessage{}
	if err = proto.Unmarshal(buf, m); err != nil {
		err = errors.Wrap(err, "unmarshalling")
		return
	}

	index = m.Index
	shard = m.Shard
	return

}
func (pb *pbTranslator) DecodeCreateIndex(buf []byte) (err error, index string) {
	//if buf[0] == messageTypeCreateIndex:
	m := &CreateIndexMessage{}
	if err = proto.Unmarshal(buf, m); err != nil {
		err = errors.Wrap(err, "unmarshalling")
		return
	}
	index = m.Index
	return
}
func (pb *pbTranslator) DecodeDeleteIndex(buf []byte) (err error, index string) {
	//if buf[0] == messageTypeDeleteIndex:
	m := &DeleteIndexMessage{}
	if err = proto.Unmarshal(buf, m); err != nil {
		err = errors.Wrap(err, "unmarshalling")
		return
	}
	index = m.Index
	return
}
func (pb *pbTranslator) DecodeCreateField(buf []byte) (err error, index, field string, opt pilosa.FieldOptions) {
	//if buf[0] == messageTypeCreateField:
	m := &CreateFieldMessage{}
	if err = proto.Unmarshal(buf, m); err != nil {
		err = errors.Wrap(err, "unmarshalling")
		return
	}
	index = m.Index
	field = m.Field

	return
}
func (pb *pbTranslator) DecodeDeleteField(buf []byte) (err error, index, field string) {
	//if buf[0] messageTypeDeleteField:
	m := &DeleteFieldMessage{}
	if err := proto.Unmarshal(buf, m); err != nil {
		err = errors.Wrap(err, "unmarshalling")
		return
	}
	index = m.Index
	field = m.Field
	return
}
func (pb *pbTranslator) DecodeCreateView(buf []byte) (err error, index, field, view string) {
	//if buf[0] messageTypeCreateView:
	m := &CreateViewMessage{}
	if err := proto.Unmarshal(buf, m); err != nil {
		err = errors.Wrap(err, "unmarshalling")
		return
	}
	index = m.Index
	field = m.Field
	view = m.View
	return

	return err, m.Index, m.Field, m.View
}

func (pb *pbTranslator) DecodeDeleteView(buf []byte) (err error, index, field, view string) {
	//if buf[0]== messageTypeDeleteView:
	m := &DeleteViewMessage{}
	if err := proto.Unmarshal(buf, m); err != nil {
		err = errors.Wrap(err, "unmarshalling")
		return
	}
	index = m.Index
	field = m.Field
	view = m.View
	return
}

func (pb *pbTranslator) DecodeClusterStatus(buf []byte) (err error, obj string) {
	//if buf[0]== messageTypeClusterStatus:
	m := &ClusterStatus{}
	if err = proto.Unmarshal(buf, m); err != nil {
		err = errors.Wrap(err, "unmarshalling")
		return
	}

	return
}
func (pb *pbTranslator) DecodeResizeInstruction(buf []byte) (err error, obj string) {
	//if buf[0]== messageTypeResizeInstruction:
	m := &ResizeInstruction{}
	if err := proto.Unmarshal(buf, m); err != nil {
		err = errors.Wrap(err, "unmarshalling")
		return
	}
	return
}
func (pb *pbTranslator) DecodeResizeInstructionComplete(buf []byte) (err error, obj string) {
	//if buf[0]== messageTypeResizeInstructionComplete:
	m = &ResizeInstructionComplete{}
	if err := proto.Unmarshal(buf, m); err != nil {
		err = errors.Wrap(err, "unmarshalling")
		return
	}
	return
}
func (pb *pbTranslator) DecodeSetCoordinator(buf []byte) (err error, node string) {
	//if buf[0]== messageTypeSetCoordinator:
	m := &SetCoordinatorMessage{}
	if err := proto.Unmarshal(buf, m); err != nil {
		err = errors.Wrap(err, "unmarshalling")
		return
	}
	return
}

func (pb *pbTranslator) DecodeUpdateCoordinator(buf []byte) (err error, node string) {
	//if buf[0]== messageTypeUpdateCoordinator:
	m := &UpdateCoordinatorMessage{}
	if err := proto.Unmarshal(buf, m); err != nil {
		err = errors.Wrap(err, "unmarshalling")
		return
	}
	return
}

func (pb *pbTranslator) DecodeNodeState(buf []byte) (err error, node, state string) {
	//if buf[0]== messageTypeNodeState:
	m := &NodeStateMessage{}
	if err := proto.Unmarshal(buf, m); err != nil {
		err = errors.Wrap(err, "unmarshalling")
		return
	}
	return
}
func (pb *pbTranslator) DecodeNodeEvent(buf []byte) (err error, node, state string) {
	//if buf[0]== messageTypeNodeEvent:
	m := &NodeEventMessage{}
	if err := proto.Unmarshal(buf, m); err != nil {
		err = errors.Wrap(err, "unmarshalling")
		return
	}
	return

}
