package main

import (
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	"bringyour.com/inspect/data"
	"github.com/oklog/ulid/v2"
)

func main() {
	// pcapFile := "data/1tls.pcapng"
	// pcapFile := "data/capture.pcap"
	// pcapFile := "data/client hellos.pcapng"

	if len(os.Args) != 2 {
		log.Fatalf("Usage:  %s INSPECT_MODE\n", os.Args[0])
	}
	fname := os.Args[1]

	dataPath := "data/capture.pcap"
	savePath := "data/test_transports.pb"

	switch fname {
	case "parse_pcap", "p":
		data.PcapToTransportFiles(dataPath, savePath)
	case "display_transports", "dt":
		data.DisplayTransports(savePath)
	case "test", "t":
		test(savePath)
	default:
		testIntervals()
		// log.Fatalf("Unknown mode: %s", fname)
	}
}

type interval struct {
	tid   ulid.ULID
	start uint64
	end   uint64
}

type coOccurrence struct {
	cMap map[ulid.ULID]map[ulid.ULID]uint64
}

func (c *coOccurrence) init() {
	c.cMap = make(map[ulid.ULID]map[ulid.ULID]uint64, 0)
}

func (c *coOccurrence) add(intv1 *interval, intv2 *interval) {
	min_ends := min(intv1.end, intv2.end)
	max_starts := max(intv1.start, intv2.start)
	// handle overflow
	if min_ends <= max_starts {
		return // no overlap (keep default value of 0)
	}
	overlap := min_ends - max_starts

	switch intv1.tid.Compare(intv2.tid) {
	case -1: // intv1.tid < intv2.tid
		if _, ok := c.cMap[intv1.tid]; !ok {
			c.cMap[intv1.tid] = make(map[ulid.ULID]uint64, 0)
		}
		c.cMap[intv1.tid][intv2.tid] = overlap
	case 1: // intv1.tid > intv2.tid
		if _, ok := c.cMap[intv2.tid]; !ok {
			c.cMap[intv2.tid] = make(map[ulid.ULID]uint64, 0)
		}
		c.cMap[intv2.tid][intv1.tid] = overlap
	}
	// do nothing when intv1.tid == intv2.tid (no overlap with itself so keep default of 0)
}

func (c *coOccurrence) get(intv1 *interval, intv2 *interval) uint64 {
	// if value doesnt exist then 0 value is returned (which is desired)
	if intv1.tid.Compare(intv2.tid) < 0 {
		return c.cMap[intv1.tid][intv2.tid]
	}
	return c.cMap[intv2.tid][intv1.tid]
}

func test(savePath string) {
	// need to make sure that transport ids are different

	records, err := data.LoadTransportsFromFiles(savePath)
	if err != nil {
		log.Fatalf("Error loading transports: %v", err)
	}

	intervals := make([]*interval, 0)

	for _, record := range records {
		intv := &interval{
			tid:   ulid.ULID(record.Open.TransportId), // should be the same as Close.TransportId
			start: record.Open.OpenTime,
		}

		if record.Close != nil {
			intv.end = record.Close.CloseTime
		} else {
			var lastWriteTime uint64 = 0
			if len(record.Writes) > 0 {
				lastWriteTime = record.Writes[len(record.Writes)-1].WriteToBufferEndTime
			}
			var lastReadTime uint64 = 0
			if len(record.Reads) > 0 {
				lastReadTime = record.Reads[len(record.Reads)-1].ReadFromBufferEndTime
			}
			intv.end = max(lastWriteTime, lastReadTime, record.Open.OpenTime)
		}

		if intv.end < intv.start {
			log.Fatalf("End time is less than start time for transport id: %x", intv.tid)
		}

		intervals = append(intervals, intv)
	}

	sort.Slice(intervals, func(i, j int) bool {
		// if intervals[i].start == intervals[j].start {
		// 	return intervals[i].end < intervals[j].end
		// }
		return intervals[i].start < intervals[j].start
	})

	for _, intv := range intervals {
		startTime := time.Unix(0, int64(intv.start)).UTC()
		endTime := time.Unix(0, int64(intv.end)).UTC()

		fmt.Printf("TransportId: %x, Start: %s, End: %s\n", intv.tid, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))
	}

	var cooc coOccurrence
	cooc.init()

	i := 0
	for _, intv1 := range intervals {
		for _, intv2 := range intervals {
			if intv1.end < intv2.start {
				// no overlap since intervals are sorted by start time
				break
			}
			cooc.add(intv1, intv2)
			i++
		}
	}
	fmt.Printf("Total overlap checks (for %d intervals): %d\n", len(intervals), i)

	cooc.testOverlaps(intervals)
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

	var cooc coOccurrence
	cooc.init()

	for _, intv1 := range intervals {
		for _, intv2 := range intervals {
			if intv1.end < intv2.start {
				// no overlap since intervals are sorted by start time
				break
			}
			cooc.add(intv1, intv2)
		}
	}

	cooc.testOverlaps(intervals)
}

func (c *coOccurrence) testOverlaps(intvs []*interval) {
	for _, intv1 := range intvs {
		for _, intv2 := range intvs {
			overlap := c.get(intv1, intv2)
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
