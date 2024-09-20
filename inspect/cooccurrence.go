package main

import (
	"fmt"
	"os"

	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/proto"

	"bringyour.com/protocol"
)

type coOccurrenceData map[ulid.ULID]map[ulid.ULID]uint64

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

func (c *coOccurrence) CalcAndSet(ov1 Overlap, ov2 Overlap) {
	tid1 := ov1.TID()
	tid2 := ov2.TID()

	if tid1.Compare(tid2) == 0 { // tid1 == tid2
		return // no overlap with itself
	}

	totalOverlap := ov1.Overlap(ov2)
	if totalOverlap == 0 {
		return
	}

	switch tid1.Compare(tid2) {
	case -1: // tid1 < tid2
		if _, ok := (*c.cMap)[tid1]; !ok {
			(*c.cMap)[tid1] = make(map[ulid.ULID]uint64, 0)
		}
		(*c.cMap)[tid1][tid2] = totalOverlap
	case 1: // tid1 > tid2
		if _, ok := (*c.cMap)[tid2]; !ok {
			(*c.cMap)[tid2] = make(map[ulid.ULID]uint64, 0)
		}
		(*c.cMap)[tid2][tid1] = totalOverlap
	}
}

func (c *coOccurrence) Get(ov1 Overlap, ov2 Overlap) uint64 {
	tid1 := ov1.TID()
	tid2 := ov2.TID()

	// if value doesnt exist then 0 value is returned (which is desired)
	if tid1.Compare(tid2) < 0 {
		return (*c.cMap)[tid1][tid2]
	}
	return (*c.cMap)[tid2][tid1]
}

func (c *coOccurrence) SaveData(dataPath string) error {
	// file, err := os.Create(dataPath)
	// if err != nil {
	// 	return err
	// }
	// defer file.Close()

	// return gob.NewEncoder(file).Encode(c.cMap)
	//

	outerMaps := make([]*protocol.OuterMap, 0)

	// for _, ts1 := range allTimestamps {
	// 	for _, ts2 := range allTimestamps {
	// 		a := cooc.Get(ts1, ts2)
	// 		b := cooc.Get(ts2, ts1)
	// 		if a != b {
	// 			fmt.Println("WHYYY")
	// 		}
	// 	}
	// }

	for outerKey, innerMap := range *c.cMap {
		outer := &protocol.OuterMap{
			Key: outerKey[:], // Convert [16]byte to []byte
		}

		for innerKey, value := range innerMap {
			outer.InnerMap = append(outer.InnerMap, &protocol.InnerMap{
				Key:   innerKey[:], // Convert [16]byte to []byte
				Value: value,
			})
		}

		outerMaps = append(outerMaps, outer)
	}

	dataToSave := &protocol.OuterMaps{
		OuterMap: outerMaps,
	}

	out, err := proto.Marshal(dataToSave)
	if err != nil {
		return err
	}

	return os.WriteFile(dataPath, out, 0644)
}

func (c *coOccurrence) LoadData(dataPath string) error {
	// file, err := os.Open(dataPath)
	// if err != nil {
	// 	return err
	// }
	// defer file.Close()

	// return gob.NewDecoder(file).Decode(c.cMap)
	//

	// Read the file
	data, err := os.ReadFile(dataPath)
	if err != nil {
		return fmt.Errorf("could not read file: %w", err)
	}

	// Unmarshal the data into OuterMaps
	outerMaps := &protocol.OuterMaps{}
	if err := proto.Unmarshal(data, outerMaps); err != nil {
		return fmt.Errorf("could not unmarshal data: %w", err)
	}

	// reconstruct the nested map structure
	result := make(coOccurrenceData, 0)

	for _, outer := range outerMaps.OuterMap {
		outerKey := [16]byte{}
		copy(outerKey[:], outer.Key) // Convert []byte back to [16]byte

		innerMap := make(map[ulid.ULID]uint64)
		for _, inner := range outer.InnerMap {
			innerKey := [16]byte{}
			copy(innerKey[:], inner.Key) // Convert []byte back to [16]byte
			innerMap[innerKey] = inner.Value
		}

		result[outerKey] = innerMap
	}

	c.cMap = &result
	return nil
}
