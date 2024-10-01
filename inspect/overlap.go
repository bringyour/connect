package main

import (
	"sort"
)

type Overlap interface {
	SID() sessionID
	Overlap(other Overlap) uint64
	// assuming this overlap starts before the other overlap
	// returns true if there is no possible overlap between this overlap and the other overlap or any other overlap that starts later than the other overlap
	NoFutureOverlap(other Overlap) bool
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

func (int1 *interval) NoFutureOverlap(other Overlap) bool {
	int2 := other.(*interval)
	return int1.end < int2.start
}

// implements Overlap interface
type timestamps struct {
	sid           sessionID
	times         []uint64
	overlapFuncts OverlapFunctions // calculates overlap between two lists of times
}

func (ts *timestamps) SID() sessionID {
	return ts.sid
}

func (ts1 *timestamps) Overlap(other Overlap) uint64 {
	ts2 := other.(*timestamps)
	return ts1.overlapFuncts.CalculateOverlap(ts1.times, ts2.times)
}

func (ts1 *timestamps) NoFutureOverlap(other Overlap) bool {
	ts2 := other.(*timestamps)
	return ts1.overlapFuncts.NoPossibleOverlap(ts1.times, ts2.times)
}

const NS_IN_SEC = uint64(1e9) // 1 * NS_TO_SEC = 1 second

// convert timestamp to float representing seconds of timestamp
func TimestampInSeconds(timestamp uint64) float64 {
	return float64(timestamp) / float64(NS_IN_SEC)
}

type OverlapFunctions interface {
	CalculateOverlap([]uint64, []uint64) uint64
	// assuming the lists of times are sorted in ascending order
	// returns true if there is no possible overlap between times1 and times2 (or any other list that starts later than times2)
	NoPossibleOverlap(times1 []uint64, times2 []uint64) bool
}

type FixedMarginOverlap struct {
	margin uint64 // fixed margin in nanoseconds
}

func (fmo *FixedMarginOverlap) CalculateOverlap(times1 []uint64, times2 []uint64) uint64 {
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
			start := t - fmo.margin
			end := t + fmo.margin
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

func (fmo *FixedMarginOverlap) NoPossibleOverlap(times1 []uint64, times2 []uint64) bool {
	if len(times1) == 0 || len(times2) == 0 {
		return true
	}
	return times1[len(times1)-1]+fmo.margin < times2[0]-fmo.margin
}
