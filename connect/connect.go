package connect

import (
	"errors"
	// "log"
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/oklog/ulid/v2"

	"bringyour.com/protocol"
)

const MaxMultihopLength = 8

// id for message to/from the platform
var ControlId = Id{}

// TODO consider having directed transfer paths
// TODO SourceTransferPath, DestinationTransferPath
// TODO this would avoid the need to check the "masks"

// there are three types of transfer paths:
//  1. a full path, which can have either source id and destination id, or stream id
//  2. a source, which can have either source id or stream id.
//     This is called the "source mask".
//  3. a destination, which can have either destination id or stream id.
//     This is called the "destination mask".
//
// comparable
type TransferPath struct {
	SourceId      Id
	DestinationId Id
	StreamId      Id
}

func DestinationId(destinationId Id) TransferPath {
	return TransferPath{
		DestinationId: destinationId,
	}
}

func SourceId(sourceId Id) TransferPath {
	return TransferPath{
		SourceId: sourceId,
	}
}

func StreamId(streamId Id) TransferPath {
	return TransferPath{
		StreamId: streamId,
	}
}

func NewTransferPath(sourceId Id, destinationId Id, streamId Id) (path TransferPath) {
	path.StreamId = streamId
	if (path.StreamId == Id{}) {
		path.SourceId = sourceId
		path.DestinationId = destinationId
	}
	return
}

func TransferPathFromProtobuf(
	protoTransferPath *protocol.TransferPath,
) (path TransferPath, err error) {
	return TransferPathFromBytes(
		protoTransferPath.SourceId,
		protoTransferPath.DestinationId,
		protoTransferPath.StreamId,
	)
}

func TransferPathFromBytes(
	sourceIdBytes []byte,
	destinationIdBytes []byte,
	streamIdBytes []byte,
) (path TransferPath, err error) {
	if streamIdBytes != nil {
		path.StreamId, err = IdFromBytes(streamIdBytes)
		if err != nil {
			return
		}
	}
	if (path.StreamId == Id{}) {
		if sourceIdBytes != nil {
			path.SourceId, err = IdFromBytes(sourceIdBytes)
			if err != nil {
				return
			}
		}
		if destinationIdBytes != nil {
			path.DestinationId, err = IdFromBytes(destinationIdBytes)
			if err != nil {
				return
			}
		}
	}
	return
}

func (self TransferPath) IsControlSource() bool {
	return self.IsSourceMask() && !self.IsStream() && self.SourceId == ControlId
}

func (self TransferPath) IsControlDestination() bool {
	return self.IsDestinationMask() && !self.IsStream() && self.DestinationId == ControlId
}

func (self TransferPath) IsStream() bool {
	return self.StreamId != Id{}
}

func (self TransferPath) IsSourceMask() bool {
	if self.IsStream() {
		return self.SourceId == Id{} && self.DestinationId == Id{}
	} else {
		return self.DestinationId == Id{}
	}
}

func (self TransferPath) IsDestinationMask() bool {
	if self.IsStream() {
		return self.SourceId == Id{} && self.DestinationId == Id{}
	} else {
		return self.SourceId == Id{}
	}
}

func (self TransferPath) SourceMask() TransferPath {
	return TransferPath{
		SourceId: self.SourceId,
		StreamId: self.StreamId,
	}
}

func (self TransferPath) DestinationMask() TransferPath {
	return TransferPath{
		DestinationId: self.DestinationId,
		StreamId:      self.StreamId,
	}
}

func (self TransferPath) Reverse() TransferPath {
	return TransferPath{
		SourceId:      self.DestinationId,
		DestinationId: self.SourceId,
		StreamId:      self.StreamId,
	}
}

func (self TransferPath) AddSource(sourceId Id) TransferPath {
	if self.IsStream() {
		return self
	}
	return TransferPath{
		SourceId:      sourceId,
		DestinationId: self.DestinationId,
	}
}

func (self TransferPath) AddDestination(destinationId Id) TransferPath {
	if self.IsStream() {
		return self
	}
	return TransferPath{
		SourceId:      self.SourceId,
		DestinationId: destinationId,
	}
}

func (self TransferPath) String() string {
	if (self.StreamId != Id{}) {
		return fmt.Sprintf("s(%s)", self.StreamId)
	} else {
		return fmt.Sprintf("%s->%s", self.SourceId, self.DestinationId)
	}
}

func (self TransferPath) ToProtobuf() *protocol.TransferPath {
	protoTransferPath := &protocol.TransferPath{}
	if self.IsStream() {
		protoTransferPath.StreamId = self.StreamId.Bytes()
	} else {
		protoTransferPath.SourceId = self.SourceId.Bytes()
		protoTransferPath.DestinationId = self.DestinationId.Bytes()
	}
	return protoTransferPath
}

// comparable
type Id [16]byte

func NewId() Id {
	return Id(ulid.Make())
}

func IdFromBytes(idBytes []byte) (Id, error) {
	if len(idBytes) != 16 {
		return Id{}, errors.New("Id must be 16 bytes")
	}
	return Id(idBytes), nil
}

func RequireIdFromBytes(idBytes []byte) Id {
	id, err := IdFromBytes(idBytes)
	if err != nil {
		panic(err)
	}
	return id
}

func ParseId(idStr string) (Id, error) {
	return parseUuid(idStr)
}

func (self Id) Bytes() []byte {
	return self[0:16]
}

func (self Id) LessThan(b Id) bool {
	return self.Cmp(b) < 0
}

func (self Id) Cmp(b Id) int {
	for i, v := range self {
		if v < b[i] {
			return -1
		}
		if b[i] < v {
			return 1
		}
	}
	return 0
}

func (self Id) String() string {
	return encodeUuid(self)
}

func (self *Id) MarshalJSON() ([]byte, error) {
	var buf [16]byte
	copy(buf[0:16], self[0:16])
	var buff bytes.Buffer
	buff.WriteByte('"')
	buff.WriteString(encodeUuid(buf))
	buff.WriteByte('"')
	b := buff.Bytes()
	return b, nil
}

func (self *Id) UnmarshalJSON(src []byte) error {
	if len(src) != 38 {
		return fmt.Errorf("invalid length for UUID: %v", len(src))
	}
	buf, err := parseUuid(string(src[1 : len(src)-1]))
	if err != nil {
		return err
	}
	*self = buf
	return nil
}

func parseUuid(src string) (dst [16]byte, err error) {
	switch len(src) {
	case 36:
		src = src[0:8] + src[9:13] + src[14:18] + src[19:23] + src[24:]
	case 32:
		// dashes already stripped, assume valid
	default:
		// assume invalid.
		return dst, fmt.Errorf("cannot parse UUID %v", src)
	}

	buf, err := hex.DecodeString(src)
	if err != nil {
		return dst, err
	}

	copy(dst[:], buf)
	return dst, err
}

func encodeUuid(src [16]byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", src[0:4], src[4:6], src[6:8], src[8:10], src[10:16])
}

// comparable
type MultiHopId struct {
	ids [MaxMultihopLength]Id
	len int
}

func NewMultiHopId(ids ...Id) (MultiHopId, error) {
	if MaxMultihopLength < len(ids) {
		return MultiHopId{}, fmt.Errorf("Multihop length exceeds maximum: %d < %d", MaxMultihopLength, len(ids))
	}
	multiHopId := MultiHopId{
		len: len(ids),
	}
	for i, id := range ids {
		multiHopId.ids[i] = id
	}
	return multiHopId, nil
}

func RequireMultiHopId(ids ...Id) MultiHopId {
	multiHopId, err := NewMultiHopId(ids...)
	if err != nil {
		panic(err)
	}
	return multiHopId
}

func MultiHopIdFromBytes(multiHopIdBytes [][]byte) (MultiHopId, error) {
	ids := make([]Id, len(multiHopIdBytes))
	for i, idBytes := range multiHopIdBytes {
		if len(idBytes) != 16 {
			return MultiHopId{}, errors.New("Id must be 16 bytes")
		}
		ids[i] = Id(idBytes)
	}
	return NewMultiHopId(ids...)
}

func RequireMultiHopIdFromBytes(multiHopIdBytes [][]byte) MultiHopId {
	multiHopId, err := MultiHopIdFromBytes(multiHopIdBytes)
	if err != nil {
		panic(err)
	}
	return multiHopId
}

func (self MultiHopId) Len() int {
	return self.len
}

func (self MultiHopId) Ids() []Id {
	return self.ids[0:self.len]
}

func (self MultiHopId) Bytes() [][]byte {
	idsBytes := make([][]byte, self.len)
	for i := 0; i < self.len; i += 1 {
		idsBytes[i] = self.ids[i].Bytes()
	}
	return idsBytes
}

func (self MultiHopId) Tail() Id {
	if self.len == 0 {
		panic(errors.New("Cannot call tail on empty multi hop id."))
	}
	return self.ids[self.len-1]
}

func (self MultiHopId) SplitTail() (MultiHopId, Id) {
	if self.len == 0 {
		panic(errors.New("Cannot call split tail on empty multi hop id."))
	}
	if self.len == 1 {
		return MultiHopId{}, self.ids[0]
	}
	intermediaryIds := MultiHopId{
		len: self.len - 1,
	}
	if 0 < self.len-1 {
		copy(intermediaryIds.ids[0:self.len-1], self.ids[0:self.len-1])
	}
	return intermediaryIds, self.ids[self.len-1]
}

func (self MultiHopId) String() string {
	parts := []string{}
	for i := 0; i < self.len; i += 1 {
		parts = append(parts, self.ids[i].String())
	}
	return fmt.Sprintf("[%s]", strings.Join(parts, ","))
}

// use this type when counting bytes
type ByteCount = int64

func kib(c ByteCount) ByteCount {
	return c * ByteCount(1024)
}

func mib(c ByteCount) ByteCount {
	return c * ByteCount(1024) * ByteCount(1024)
}

func gib(c ByteCount) ByteCount {
	return c * ByteCount(1024) * ByteCount(1024) * ByteCount(1024)
}
