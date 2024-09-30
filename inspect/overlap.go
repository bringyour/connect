package main

import (
	"sort"
)

type Overlap interface {
	SID() sessionID
	Overlap(other Overlap) uint64
}

// implements Overlap interface
type interval struct {
	sid   sessionID
	start uint64
	end   uint64
}

func (intv *interval) SID() sessionID {
	return intv.sid
}

func (int1 *interval) Overlap(other Overlap) uint64 {
	int2 := other.(*interval)
	min_ends := min(int1.end, int2.end)
	max_starts := max(int1.start, int2.start)
	if min_ends <= max_starts { // handle overflow
		return 0 // no overlap
	}
	return min_ends - max_starts
}

// implements Overlap interface
type timestamps struct {
	sid         sessionID
	times       []uint64
	overlapFunc func([]uint64, []uint64) uint64 // calculates overlap between two lists of times
}

func (ts *timestamps) SID() sessionID {
	return ts.sid
}

// func (ts *timestamps) AddTimes(times []uint64) {
// 	if len(times) == 0 {
// 		ts.times = times
// 		return
// 	}
// 	// get first and last times from curent list and new list
// 	currentFirst := times[0]
// 	currentLast := times[len(times)-1]
// 	possibleFirst := ts.times[0]
// 	possibleLast := ts.times[len(ts.times)-1]

// 	// get new first
// 	newFirst := min(currentFirst, possibleFirst)
// 	// get new last
// 	newLast := max(currentLast, possibleLast)
// }

func (ts1 *timestamps) Overlap(other Overlap) uint64 {
	ts2 := other.(*timestamps)
	return ts1.overlapFunc(ts1.times, ts2.times)
}

const NANO_IN_SEC = float64(1e9)
const FIXED_MARGIN = 1 // * NANO_IN_SEC

// convert timestamp to float representing seconds of timestamp
func TsFloat(timestamp uint64) float64 {
	return float64(timestamp) / NANO_IN_SEC
}

func FixedMarginOverlap(times1 []uint64, times2 []uint64, fixedMargin uint64) uint64 {
	// define a fimex margin Event type for the sweep line algorithm
	type fmEvent struct {
		eTime   uint64
		eType   uint8 // 1 for start, 2 for end
		eListID uint8 // 1 for times1, 2 for times2
	}

	var events []fmEvent

	// helper function to add events to the slice
	addEvents := func(times []uint64, listID uint8) {
		for _, t := range times {
			start := t - fixedMargin
			end := t + fixedMargin
			events = append(events, fmEvent{eTime: start, eType: 1, eListID: listID}) // Start event
			events = append(events, fmEvent{eTime: end, eType: 2, eListID: listID})   // End event
		}
	}

	// add events for both lists of times
	addEvents(times1, 1)
	addEvents(times2, 2)

	// sort events by time; if times are the same, end events should come before start events
	sort.Slice(events, func(i, j int) bool {
		if events[i].eTime == events[j].eTime {
			if events[i].eType == events[j].eType {
				return events[i].eListID < events[j].eListID
			}
			return events[i].eType < events[j].eType
		}
		return events[i].eTime < events[j].eTime
	})

	// sweep line algorithm to calculate overlap
	totalOverlap := uint64(0)
	activeTimes1 := 0
	activeTimes2 := 0
	lastTime := uint64(0)

	for _, e := range events {
		if activeTimes1 > 0 && activeTimes2 > 0 {
			totalOverlap += e.eTime - lastTime
		}
		if e.eType == 1 {
			if e.eListID == 1 {
				activeTimes1++
			} else {
				activeTimes2++
			}
		} else {
			if e.eListID == 1 {
				activeTimes1--
			} else {
				activeTimes2--
			}
		}
		lastTime = e.eTime
	}

	return totalOverlap
}
