package main

import (
	"fmt"
	"os"

	"google.golang.org/protobuf/proto"

	"bringyour.com/protocol"
)

type sessionID string

// Compare returns -1 if s < other, 0 if s == other, and 1 if s > other.
func (s sessionID) Compare(other sessionID) int {
	switch {
	case s < other:
		return -1
	case s > other:
		return 1
	default:
		return 0
	}
}

type coOccurrenceData map[sessionID]map[sessionID]uint64

// used to precompute distances for clustering
type coOccurrence struct {
	cMap *coOccurrenceData
}

func NewCoOccurrence(cmapData *coOccurrenceData) *coOccurrence {
	if cmapData == nil {
		_cmapData := make(coOccurrenceData, 0)
		cmapData = &_cmapData
	}
	return &coOccurrence{
		cMap: cmapData,
	}
}

func (c *coOccurrence) SetInnerKeys(sid sessionID) {
	(*c.cMap)[sid] = make(map[sessionID]uint64, 0)
}

func (c *coOccurrence) CalcAndSet(ov1 Overlap, ov2 Overlap) {
	sid1 := ov1.SID()
	sid2 := ov2.SID()

	if sid1.Compare(sid2) == 0 { // sid1 == sid2
		return // no overlap with itself
	}

	totalOverlap := ov1.Overlap(ov2)
	if totalOverlap == 0 {
		return // do not record 0 overlap
	}

	switch sid1.Compare(sid2) {
	case -1: // sid1 < sid2
		if _, ok := (*c.cMap)[sid1]; !ok {
			(*c.cMap)[sid1] = make(map[sessionID]uint64, 0)
		}
		(*c.cMap)[sid1][sid2] = totalOverlap
	case 1: // sid1 > sid2
		if _, ok := (*c.cMap)[sid2]; !ok {
			(*c.cMap)[sid2] = make(map[sessionID]uint64, 0)
		}
		(*c.cMap)[sid2][sid1] = totalOverlap
	}
}

func (c *coOccurrence) Get(ov1 Overlap, ov2 Overlap) uint64 {
	sid1 := ov1.SID()
	sid2 := ov2.SID()

	// if value doesnt exist then 0 value is returned (which is desired)
	if sid1.Compare(sid2) < 0 {
		return (*c.cMap)[sid1][sid2]
	}
	return (*c.cMap)[sid2][sid1]
}

func (c *coOccurrence) SaveData(dataPath string) error {
	coocData := make([]*protocol.CoocOuter, 0)

	for outerSid, coocInner := range *c.cMap {
		outer := &protocol.CoocOuter{
			Sid: string(outerSid),
		}

		for innerSid, overlap := range coocInner {
			outer.CoocInner = append(outer.CoocInner, &protocol.CoocInner{
				Sid:     string(innerSid),
				Overlap: overlap,
			})
		}

		coocData = append(coocData, outer)
	}

	dataToSave := &protocol.CooccurrenceData{
		CoocOuter: coocData,
	}

	out, err := proto.Marshal(dataToSave)
	if err != nil {
		return err
	}

	return os.WriteFile(dataPath, out, 0644)
}

func (c *coOccurrence) LoadData(dataPath string) error {
	data, err := os.ReadFile(dataPath)
	if err != nil {
		return fmt.Errorf("could not read file: %w", err)
	}

	coocData := &protocol.CooccurrenceData{}
	if err := proto.Unmarshal(data, coocData); err != nil {
		return fmt.Errorf("could not unmarshal data: %w", err)
	}

	result := make(coOccurrenceData, 0)

	for _, outer := range coocData.CoocOuter {
		outerSid := sessionID(outer.Sid)

		innerMap := make(map[sessionID]uint64)
		for _, inner := range outer.CoocInner {
			innerSid := sessionID(inner.Sid)
			innerMap[innerSid] = inner.Overlap
		}

		result[outerSid] = innerMap
	}

	c.cMap = &result
	return nil
}
