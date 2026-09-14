package connect

import (
	"errors"
	// "log"
	"fmt"
	"strconv"
	"strings"

	"github.com/urnetwork/connect/v2026/protocol"
)

// The maximum number of intermediary clients in one stream. A MultiHopId also
// includes its final destination, so its maximum stored length is one larger.
const MaxMultihopLength = 8

// Internal storage adds the final destination to the intermediary limit.
const maximumMultiHopIdLength = MaxMultihopLength + 1

// v1: original
// v2: 2025-05-28 to optimize memory usage. Breaks compatibility with v1
//
//	Most clients need to be able to read v2 before we turn this on.
const DefaultProtocolVersion = 2

// v1: the first versioned stream version
//
//	this version requires that TransferPath allow simultaneous source, destination, and stream ids
const DefaultStreamVersion = 1

// id for message to/from the platform
var ControlId = Id{}

// TODO consider having directed transfer paths
// TODO SourceTransferPath, DestinationTransferPath
// TODO this would avoid the need to check the "masks"

// there are four types of transfer paths:
//  1. a full path, which will have source id, destination id, and optional stream id
//  2. a full path without stream, which will have source id and/or destination id, but no stream id.
//     This is called the "local mask".
//  3. a source, which will have source id and optional stream id.
//     This is called the "source mask".
//  4. a destination, which will have destination id and optional stream id.
//     This is called the "destination mask".
// Normally a local mask should be stored in the protobuf message transfer path,
// and the destination mask should be used to match routes.

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
	path.SourceId = sourceId
	path.DestinationId = destinationId
	path.StreamId = streamId
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
	if streamIdBytes != nil {
		path.StreamId, err = IdFromBytes(streamIdBytes)
		if err != nil {
			return
		}
	}
	return
}

func (self TransferPath) IsControlSource() bool {
	return self.SourceId == ControlId && (self.DestinationId == Id{}) && (self.StreamId == Id{})
}

func (self TransferPath) IsControlDestination() bool {
	return self.DestinationId == ControlId && (self.SourceId == Id{}) && (self.StreamId == Id{})
}

func (self TransferPath) IsStream() bool {
	return self.StreamId != Id{}
}

func (self TransferPath) IsSourceMask() bool {
	return self.DestinationId == Id{}
}

func (self TransferPath) IsDestinationMask() bool {
	return self.SourceId == Id{}
}

func (self TransferPath) IsLocalMask() bool {
	return self.StreamId == Id{}
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

func (self TransferPath) LocalMask() TransferPath {
	return TransferPath{
		SourceId:      self.SourceId,
		DestinationId: self.DestinationId,
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
	return TransferPath{
		SourceId:      sourceId,
		DestinationId: self.DestinationId,
		StreamId:      self.StreamId,
	}
}

func (self TransferPath) AddDestination(destinationId Id) TransferPath {
	return TransferPath{
		SourceId:      self.SourceId,
		DestinationId: destinationId,
		StreamId:      self.StreamId,
	}
}

func (self TransferPath) String() string {
	var spart string
	var dpart string
	if (self.StreamId != Id{}) {
		spart = fmt.Sprintf("s(%s)", self.StreamId)
	}
	if (self.SourceId != Id{}) || (self.DestinationId != Id{}) {
		dpart = fmt.Sprintf("%s->%s", self.SourceId, self.DestinationId)
	}
	if spart != "" && dpart != "" {
		return fmt.Sprintf("%s %s", spart, dpart)
	} else if spart != "" {
		return spart
	} else {
		return dpart
	}
}

func (self TransferPath) ToProtobuf() *protocol.TransferPath {
	protoTransferPath := &protocol.TransferPath{}
	if (self.SourceId != Id{}) {
		protoTransferPath.SourceId = self.SourceId.Bytes()
	}
	if (self.DestinationId != Id{}) {
		protoTransferPath.DestinationId = self.DestinationId.Bytes()
	}
	if (self.StreamId != Id{}) {
		protoTransferPath.StreamId = self.StreamId.Bytes()
	}
	return protoTransferPath
}

// comparable
type MultiHopId struct {
	ids [maximumMultiHopIdLength]Id
	len int
}

func NewMultiHopId(ids ...Id) (MultiHopId, error) {
	if maximumMultiHopIdLength < len(ids) {
		return MultiHopId{}, fmt.Errorf("Multihop length exceeds maximum: %d < %d", maximumMultiHopIdLength, len(ids))
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

func Kib(c ByteCount) ByteCount {
	return kib(c)
}

func kib(c ByteCount) ByteCount {
	return c * ByteCount(1024)
}

func Mib(c ByteCount) ByteCount {
	return mib(c)
}

func mib(c ByteCount) ByteCount {
	return c * ByteCount(1024) * ByteCount(1024)
}

func Gib(c ByteCount) ByteCount {
	return gib(c)
}

func gib(c ByteCount) ByteCount {
	return c * ByteCount(1024) * ByteCount(1024) * ByteCount(1024)
}

func ByteCountHumanReadable(count ByteCount) string {
	trimFloatString := func(value float64, precision int, suffix string) string {
		s := fmt.Sprintf("%."+strconv.Itoa(precision)+"f", value)
		s = strings.TrimRight(s, "0")
		s = strings.TrimRight(s, ".")
		return s + suffix
	}

	if 1024*1024*1024*1024 <= count {
		return trimFloatString(
			float64(1000*count/(1024*1024*1024*1024))/1000.0,
			2,
			"tib",
		)
	} else if 1024*1024*1024 <= count {
		return trimFloatString(
			float64(1000*count/(1024*1024*1024))/1000.0,
			2,
			"gib",
		)
	} else if 1024*1024 <= count {
		return trimFloatString(
			float64(1000*count/(1024*1024))/1000.0,
			2,
			"mib",
		)
	} else if 1024 <= count {
		return trimFloatString(
			float64(1000*count/(1024))/1000.0,
			2,
			"kib",
		)
	} else {
		return fmt.Sprintf("%db", count)
	}
}

func ParseByteCount(humanReadable string) (ByteCount, error) {
	humanReadableLower := strings.ToLower(humanReadable)
	tibLower := "tib"
	gibLower := "gib"
	mibLower := "mib"
	kibLower := "kib"
	bLower := "b"
	if strings.HasSuffix(humanReadableLower, tibLower) {
		countFloat, err := strconv.ParseFloat(
			humanReadableLower[0:len(humanReadableLower)-len(tibLower)],
			64,
		)
		if err != nil {
			return ByteCount(0), err
		}
		return ByteCount(countFloat * 1024 * 1024 * 1024 * 1024), nil
	} else if strings.HasSuffix(humanReadableLower, gibLower) {
		countFloat, err := strconv.ParseFloat(
			humanReadableLower[0:len(humanReadableLower)-len(gibLower)],
			64,
		)
		if err != nil {
			return ByteCount(0), err
		}
		return ByteCount(countFloat * 1024 * 1024 * 1024), nil
	} else if strings.HasSuffix(humanReadableLower, mibLower) {
		countFloat, err := strconv.ParseFloat(
			humanReadableLower[0:len(humanReadableLower)-len(mibLower)],
			64,
		)
		if err != nil {
			return ByteCount(0), err
		}
		return ByteCount(countFloat * 1024 * 1024), nil
	} else if strings.HasSuffix(humanReadableLower, kibLower) {
		countFloat, err := strconv.ParseFloat(
			humanReadableLower[0:len(humanReadableLower)-len(kibLower)],
			64,
		)
		if err != nil {
			return ByteCount(0), err
		}
		return ByteCount(countFloat * 1024), nil
	} else if strings.HasSuffix(humanReadableLower, bLower) {
		countFloat, err := strconv.ParseFloat(
			humanReadableLower[0:len(humanReadableLower)-len(bLower)],
			64,
		)
		if err != nil {
			return ByteCount(0), err
		}
		return ByteCount(countFloat), nil
	} else {
		countInt, err := strconv.ParseInt(humanReadableLower, 10, 63)
		if err != nil {
			return ByteCount(0), err
		}
		return ByteCount(countInt), nil
	}
}
