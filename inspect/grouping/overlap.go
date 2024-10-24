package grouping

import (
	"fmt"
	"sort"

	"gonum.org/v1/gonum/stat/distuv"
)

type Overlap interface {
	SID() SessionID
	Overlap(other Overlap) uint64
	// assuming this overlap starts before the other overlap
	// returns true if there is no possible overlap between this overlap and the other overlap or any other overlap that starts later than the other overlap
	NoFutureOverlap(other Overlap) bool
}

// implements Overlap interface
type Interval struct {
	Sid   SessionID
	Start uint64
	End   uint64
}

func (intv *Interval) SID() SessionID {
	return intv.Sid
}

func (int1 *Interval) Overlap(other Overlap) uint64 {
	int2 := other.(*Interval)
	min_ends := min(int1.End, int2.End)
	max_starts := max(int1.Start, int2.Start)
	if min_ends <= max_starts { // handle overflow
		return 0 // no overlap
	}
	return min_ends - max_starts
}

func (int1 *Interval) NoFutureOverlap(other Overlap) bool {
	int2 := other.(*Interval)
	return int1.End < int2.Start
}

// implements Overlap interface
type Timestamps struct {
	Sid           SessionID
	Times         []uint64
	OverlapFuncts OverlapFunctions // calculates overlap between two lists of times
}

func (ts *Timestamps) SID() SessionID {
	return ts.Sid
}

func (ts1 *Timestamps) Overlap(other Overlap) uint64 {
	ts2 := other.(*Timestamps)
	return ts1.OverlapFuncts.CalculateOverlap(ts1.Times, ts2.Times)
}

func (ts1 *Timestamps) NoFutureOverlap(other Overlap) bool {
	ts2 := other.(*Timestamps)
	return ts1.OverlapFuncts.NoPossibleOverlap(ts1.Times, ts2.Times)
}

const NS_IN_SEC = uint64(1e9) // 1 * NS_TO_SEC = 1 second

// convert timestamp to float representing seconds of timestamp
func TimestampInSeconds(timestamp uint64) float64 {
	return float64(timestamp) / float64(NS_IN_SEC)
}

// convert timestamp to uint64 representing nanoseconds of timestamp
// (will keep leading 9 digits after decimal point)
func TimestampInNano(timestamp float64) uint64 {
	res := timestamp * float64(NS_IN_SEC)
	if res < 0 {
		fmt.Printf("Negative timestamp (%.10f) returning 0\n", timestamp)
		return 0
	}
	return uint64(res)
}

type OverlapFunctions interface {
	CalculateOverlap([]uint64, []uint64) uint64
	// assuming the lists of times are sorted in ascending order
	// returns true if there is no possible overlap between times1 and times2 (or any other list that starts later than times2)
	NoPossibleOverlap(times1 []uint64, times2 []uint64) bool
}

type FixedMarginOverlap struct {
	Margin uint64 // fixed margin in nanoseconds
}

func (fmo *FixedMarginOverlap) CalculateOverlap(times1, times2 []uint64) uint64 {
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
			start := t - fmo.Margin
			end := t + fmo.Margin
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

func (fmo *FixedMarginOverlap) NoPossibleOverlap(times1, times2 []uint64) bool {
	if len(times1) == 0 || len(times2) == 0 {
		return true
	}
	time1 := times1[len(times1)-1]
	time2 := times2[0]
	return time1+fmo.Margin < time2-fmo.Margin
}

type GaussianOverlap struct {
	StdDev uint64 // standard deviation in nanoseconds
	Cutoff uint64 // cutoff in times of standard deviation
}

func (gausso *GaussianOverlap) gaussianOverlap(g1, g2 *distuv.Normal) float64 {
	cutoff := TimestampInSeconds(gausso.StdDev) * float64(gausso.Cutoff)
	upperLimit := min(g1.Mu, g2.Mu) + cutoff
	lowerLimit := max(g1.Mu, g2.Mu) - cutoff
	if lowerLimit >= upperLimit {
		return 0.0 // no overlap
	}

	middle := (g1.Mu + g2.Mu) / 2
	//      1  :  2  (dotted lines[.] are means of distributions 1 and 2;
	//     /.\ : /.\              double-dotted[:] line is the mid point)
	//    / . \:/ . \
	//   /  . /:\ .  \
	//  /   ./ : \.   \
	// overlap area is (twice because of symmetry)
	//the 2nd distribution's (the one with bigger mean) CDF until the mid point (v line),
	// i.e., small triangle formed by 2nd distribution and mid point
	biggerMeanGaussian := 0.0
	if g1.Mu > g2.Mu {
		biggerMeanGaussian = g1.CDF(middle)
	} else {
		biggerMeanGaussian = g2.CDF(middle)
	}
	return 2.0 * biggerMeanGaussian
}

func (gausso *GaussianOverlap) CalculateOverlap(times1, times2 []uint64) uint64 {
	if gausso.Cutoff == 0 {
		fmt.Println("Cutoff is 0, no overlap possible")
	}

	totalOverlap := 0.0 // in seconds

	makeGaussian := func(times []uint64) []*distuv.Normal {
		gaussians := make([]*distuv.Normal, len(times))
		for i, t := range times {
			// fmt.Printf("times[%v] = %v\n", i, t)
			gaussians[i] = &distuv.Normal{Mu: TimestampInSeconds(t), Sigma: TimestampInSeconds(gausso.StdDev)}
		}
		return gaussians
	}

	gaussian1 := makeGaussian(times1)
	gaussian2 := makeGaussian(times2)

	// Check overlap for each time in times1 with all times in times2
	for _, g1 := range gaussian1 {
		for _, g2 := range gaussian2 {
			overlap := gausso.gaussianOverlap(g1, g2)
			// fmt.Printf("[%v] overlap b/w %.6f and %.6f = %v\n", g1.Sigma, g1.Mu, g2.Mu, overlap)
			totalOverlap += overlap
		}
	}

	return TimestampInNano(totalOverlap) // convert to nanoseconds
}

func (gausso *GaussianOverlap) NoPossibleOverlap(times1, times2 []uint64) bool {
	if len(times1) == 0 || len(times2) == 0 {
		return true
	}
	// If the distance between means is greater than the sum of their cutoffs, no overlap is possible
	mean1 := TimestampInSeconds(times1[len(times1)-1])
	mean2 := TimestampInSeconds(times2[0])
	sCutoff := TimestampInSeconds(gausso.Cutoff)
	return mean1+sCutoff < mean2-sCutoff
}
