package grouping

import (
	"fmt"
	"os"

	"bringyour.com/protocol"

	"google.golang.org/protobuf/proto"
)

type SessionID string

// Compare returns -1 if s < other, 0 if s == other, and 1 if s > other.
func (s SessionID) Compare(other SessionID) int {
	switch {
	case s < other:
		return -1
	case s > other:
		return 1
	default:
		return 0
	}
}

type CoOccurrenceData map[SessionID]map[SessionID]uint64

// used to precompute distances for clustering
type CoOccurrence struct {
	Data *CoOccurrenceData
}

func NewCoOccurrence(cmapData *CoOccurrenceData) *CoOccurrence {
	if cmapData == nil {
		_cmapData := make(CoOccurrenceData, 0)
		cmapData = &_cmapData
	}
	return &CoOccurrence{
		Data: cmapData,
	}
}

func (c *CoOccurrence) SetOuterKey(sid SessionID) {
	(*c.Data)[sid] = make(map[SessionID]uint64, 0)
}

func (c *CoOccurrence) CalcAndSet(ov1 Overlap, ov2 Overlap) {
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
		if _, ok := (*c.Data)[sid1]; !ok {
			(*c.Data)[sid1] = make(map[SessionID]uint64, 0)
		}
		(*c.Data)[sid1][sid2] = totalOverlap
	case 1: // sid1 > sid2
		if _, ok := (*c.Data)[sid2]; !ok {
			(*c.Data)[sid2] = make(map[SessionID]uint64, 0)
		}
		(*c.Data)[sid2][sid1] = totalOverlap
	}
}

func (c *CoOccurrence) Get(sid1 SessionID, sid2 SessionID) uint64 {
	// if value doesnt exist then 0 value is returned (which is desired)
	if sid1.Compare(sid2) < 0 {
		return (*c.Data)[sid1][sid2]
	}
	return (*c.Data)[sid2][sid1]
}

func (c *CoOccurrence) SaveData(dataPath string) error {
	coocData := make([]*protocol.CoocOuter, 0)

	for outerSid, coocInner := range *c.Data {
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

func (c *CoOccurrence) LoadData(dataPath string) error {
	data, err := os.ReadFile(dataPath)
	if err != nil {
		return fmt.Errorf("could not read file: %w", err)
	}

	coocData := &protocol.CooccurrenceData{}
	if err := proto.Unmarshal(data, coocData); err != nil {
		return fmt.Errorf("could not unmarshal data: %w", err)
	}

	result := make(CoOccurrenceData, 0)

	for _, outer := range coocData.CoocOuter {
		outerSid := SessionID(outer.Sid)

		innerMap := make(map[SessionID]uint64)
		for _, inner := range outer.CoocInner {
			innerSid := SessionID(inner.Sid)
			innerMap[innerSid] = inner.Overlap
		}

		result[outerSid] = innerMap
	}

	c.Data = &result
	return nil
}
