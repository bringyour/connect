package main

import (
	"fmt"
	"log"
	"os"
	"sort"

	"bringyour.com/inspect/data"
	"github.com/oklog/ulid/v2"
	"gonum.org/v1/gonum/floats"
	"gonum.org/v1/gonum/stat"
)

func main() {
	// pcapFile := "data/1tls.pcapng"
	// pcapFile := "data/capture.pcap"
	// pcapFile := "data/client hellos.pcapng"

	if len(os.Args) != 2 {
		log.Fatalf("Usage:  %s INSPECT_MODE\n", os.Args[0])
	}
	fname := os.Args[1]

	// dataPath := "data/capture.pcap"
	dataPath := "data/100k_tcp.pcap"

	savePath := "data/test_transports.pb"
	// savePath := "data/test_2k_records.pb"

	coOccurrencePath := "data/cooccurrence_data_small.pb"
	// coOccurrencePath := "data/cooccurrence_data_2k.pb"

	switch fname {
	case "parse_pcap", "p":
		data.PcapToTransportFiles(dataPath, savePath)
	case "display_transports", "dt":
		data.DisplayTransports(savePath)
	case "timestamps", "t":
		makeTimestamps(savePath, coOccurrencePath)
	case "cluster", "c":
		cluster(coOccurrencePath)
	default:
		testIntervals()
		// log.Fatalf("Unknown mode: %s", fname)
	}
}

func makeTimestamps(savePath string, coOccurrencePath string) {
	// times1 := []uint64{100, 120, 110, 123}
	// times2 := []uint64{110, 110, 122, 115}
	// fixedMargin := uint64(10)

	// overlap := FixedMarginOverlap(times1, times2, fixedMargin)
	// fmt.Printf("Total overlap: %d\n", overlap)

	// need to make sure that transport ids are different

	records, err := data.LoadTransportsFromFiles(savePath)
	if err != nil {
		log.Fatalf("Error loading transports: %v", err)
	}

	// extract timestamps from records
	allTimestamps := make([]*timestamps, 0)
	for _, record := range records {
		times := []uint64{record.Open.OpenTime}
		for _, write := range record.Writes {
			times = append(times, write.WriteToBufferEndTime)
		}
		for _, read := range record.Reads {
			times = append(times, read.ReadFromBufferEndTime)
		}
		if record.Close != nil {
			times = append(times, record.Close.CloseTime)
		}
		ts := &timestamps{
			tid:   ulid.ULID(record.Open.TransportId),
			times: times,
			overlapFunc: func(times1 []uint64, times2 []uint64) uint64 {
				return FixedMarginOverlap(times1, times2, FIXED_MARGIN)
			},
		}
		allTimestamps = append(allTimestamps, ts)
	}
	sort.Slice(allTimestamps, func(i, j int) bool {
		return allTimestamps[i].times[0] < allTimestamps[j].times[0] // sort by open time
	})

	// populate co-occurrence map
	cooc := NewCoOccurrence(nil)
	for _, ts1 := range allTimestamps {
		for _, ts2 := range allTimestamps {
			if ts1.times[len(ts1.times)-1]+2*FIXED_MARGIN < ts2.times[0] {
				break // no overlap further ahead since intervals are sorted by open time
			}
			cooc.CalcAndSet(ts1, ts2)
		}
	}
	cooc.SaveData(coOccurrencePath)

	// print statistics about overlaps
	float64Overlaps := make([]float64, 0)
	for _, cmap := range *cooc.cMap {
		for _, v := range cmap {
			new_v := OverlapInSec(v) // convert to seconds
			if new_v > 0 {
				float64Overlaps = append(float64Overlaps, new_v)
			}
		}
	}
	fmt.Printf(`# of timestamps: %d
# non-zero overlaps: %d
	Min: %.9f
	Max: %.9f
	Mean: %.9f
	StdDev: %.9f
`,
		len(allTimestamps),
		len(float64Overlaps),
		floats.Min(float64Overlaps),
		floats.Max(float64Overlaps),
		stat.Mean(float64Overlaps, nil),
		stat.StdDev(float64Overlaps, nil),
	)
}

func testIntervals() {
	intervals := []*interval{
		{
			tid:   ulid.MustNew(3, nil),
			start: 5,
			end:   15,
		},
		{
			tid:   ulid.MustNew(2, nil),
			start: 2,
			end:   7,
		},
		{
			tid:   ulid.MustNew(1, nil),
			start: 1,
			end:   4,
		},
		{
			tid:   ulid.MustNew(0, nil),
			start: 0,
			end:   10,
		},
	}

	sort.Slice(intervals, func(i, j int) bool {
		// if intervals[i].start == intervals[j].start {
		// 	return intervals[i].end < intervals[j].end
		// }
		return intervals[i].start < intervals[j].start
	})

	for _, intv := range intervals {
		fmt.Printf("TransportId: %s, Start: %d, End: %d\n", intv.tid, intv.start, intv.end)
	}

	cooc := NewCoOccurrence(nil)

	for _, intv1 := range intervals {
		for _, intv2 := range intervals {
			if intv1.end < intv2.start {
				// no overlap since intervals are sorted by start time
				break
			}
			cooc.CalcAndSet(intv1, intv2)
		}
	}

	cooc.testOverlaps(intervals)
}

func (c *coOccurrence) testOverlaps(intvs []*interval) {
	for _, intv1 := range intvs {
		for _, intv2 := range intvs {
			overlap := c.Get(intv1, intv2)
			correctOverlap := max(0, min(int(intv1.end), int(intv2.end))-max(int(intv1.start), int(intv2.start))) // converts to int to avoid overflow
			if intv1.tid.Compare(intv2.tid) == 0 {
				correctOverlap = 0 // no overlap with itself
			}
			if int(overlap) != correctOverlap {
				log.Fatalf("Overlap calculation error for [%d,%d] and [%d,%d]: %d should be %d", intv1.start, intv1.end, intv2.start, intv2.end, overlap, correctOverlap)
			}
		}
	}
}
