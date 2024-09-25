package main

import (
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	"bringyour.com/inspect/data"
	"github.com/mpraski/clusters"
	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/proto"

	"bringyour.com/protocol"
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
	clusterPath := "data/cluster_data_small.pb"
	coOccurrencePath := "data/cooccurrence_data_small.pb"

	// dataPath := "data/100k_tcp.pcap"
	// savePath := "data/test_2k_records.pb"
	// coOccurrencePath := "data/cooccurrence_data_2k.pb"
	// clusterPath := "data/cluster_data_2k.pb"

	switch fname {
	case "parse_pcap", "p":
		data.PcapToTransportFiles(dataPath, savePath)
	case "display_transports", "dt":
		data.DisplayTransports(savePath)
	case "timestamps", "t":
		makeTimestamps(savePath, coOccurrencePath, clusterPath)
	case "cluster", "c":
		cluster(coOccurrencePath)
	default:
		testIntervals()
		// log.Fatalf("Unknown mode: %s", fname)
	}
}

func FixedMarginOverlapF(times1 []float64, times2 []float64, fixedMargin float64) float64 {
	// define a fixed margin Event type for the sweep line algorithm
	type fmEvent struct {
		eTime   float64
		eType   uint8 // 1 for start, 2 for end
		eListID uint8 // 1 for times1, 2 for times2
	}

	var events []fmEvent

	// helper function to add events to the slice
	addEvents := func(times []float64, listID uint8) {
		for _, t := range times {
			if t == 0.0 {
				continue
			}
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
	totalOverlap := 0.0
	activeTimes1 := 0
	activeTimes2 := 0
	lastTime := 0.0

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

func getClusterLookup(ulidLookup []ulid.ULID, clusters []int) map[ulid.ULID]int {
	clusterLookup := make(map[ulid.ULID]int, 0)
	for index, cluster := range clusters {
		clusterLookup[ulidLookup[index]] = cluster
	}
	return clusterLookup
}

func splitTransportInClusters(ulidLookup []ulid.ULID, clustering []int) map[int][]ulid.ULID {
	clusterTIDs := make(map[int][]ulid.ULID, 0)
	for index, cluster := range clustering {
		currentTID := ulidLookup[index]
		if clusterTransports, exists := clusterTIDs[cluster]; exists {
			clusterTransports = append(clusterTransports, currentTID)
			clusterTIDs[cluster] = clusterTransports
		} else {
			clusterTIDs[cluster] = []ulid.ULID{currentTID}
		}
	}
	return clusterTIDs
}

func updateMax(maxP *float64, timestamp uint64) (newTimestamp float64) {
	newTimestamp = TsFloat(timestamp)
	if newTimestamp > *maxP {
		*maxP = newTimestamp
	}
	return
}

func makeTimestamps(savePath string, coOccurrencePath string, clusterPath string) {
	records, err := data.LoadTransportsFromFiles(savePath)
	if err != nil {
		log.Fatalf("Error loading transports: %v", err)
	}

	ulidLookup := make([]ulid.ULID, len(records))

	// extract timestamps from records
	maxTime := -1.0
	allTimes := make([][]float64, len(records))
	i := 0
	for _, record := range records {
		times := []float64{updateMax(&maxTime, record.Open.OpenTime)}
		ulidLookup[i] = ulid.ULID(record.Open.TransportId)
		for _, write := range record.Writes {
			times = append(times, updateMax(&maxTime, write.WriteToBufferEndTime))
		}
		for _, read := range record.Reads {
			times = append(times, updateMax(&maxTime, read.ReadFromBufferEndTime))
		}
		if record.Close != nil {
			times = append(times, updateMax(&maxTime, record.Close.CloseTime))
		}
		allTimes[i] = times
		i++
	}

	start := time.Now()

	distFunc := func(a []float64, b []float64) float64 {
		res := FixedMarginOverlapF(a, b, float64(FIXED_MARGIN))
		final := 1 / (res + 1e-9)
		// fmt.Printf("%v %v\n", int64(res), final)
		return final
	}

	minpts := 2
	eps := 0.3
	// c, e := clusters.OPTICS(minpts, eps, 0.0005, 1, distFunc)
	c, e := clusters.DBSCAN(minpts, eps, 4, distFunc)
	if e != nil {
		panic(e)
	}
	if e = c.Learn(allTimes); e != nil {
		fmt.Printf("Errror from learning: %v", e)
		return
	}
	fmt.Printf("Clustered data set into %d\n", c.Sizes())
	clusterTIDs := splitTransportInClusters(ulidLookup, c.Guesses())
	// for clusterID, transports := range clusterTIDs {
	// 	fmt.Printf("Cluster %v includes %v\n", clusterID, transports)
	// }

	// // append zeros to each inner array to make them the same length
	// maxLength := 0
	// for _, times := range allTimestamps {
	// 	if len(times) > maxLength {
	// 		maxLength = len(times)
	// 	}
	// }
	// for i := range allTimestamps {
	// 	if len(allTimestamps[i]) < maxLength {
	// 		// Append zeros until the length matches maxLength
	// 		for j := len(allTimestamps[i]); j < maxLength; j++ {
	// 			allTimestamps[i] = append(allTimestamps[i], 0.0)
	// 		}
	// 	}
	// }
	// minimumClusterSize := 5
	// minimumSpanningTree := false
	// clustering, err := hdbscan.NewClustering(allTimestamps, minimumClusterSize)
	// if err != nil {
	// 	panic(err)
	// }
	// clustering = clustering.Verbose().OutlierDetection()
	// clustering.Run(distFunc, hdbscan.VarianceScore, minimumSpanningTree)
	// for _, c := range clustering.Clusters {
	// 	fmt.Printf("%v %v\n", c.Points, c.Outliers)
	// }

	fmt.Printf("Execution took %s\n", time.Since(start))

	pClusters := make([]*protocol.Cluster, 0)
	for clusterID, transports := range clusterTIDs {
		// fmt.Printf("Cluster %v includes %v\n", clusterID, transports)

		newTransports := make([][]byte, 0)
		for _, transport := range transports {
			newTransports = append(newTransports, transport.Bytes())
		}

		pCluster := &protocol.Cluster{
			ClusterID: uint64(clusterID),
			Transports: &protocol.TransportsInCluster{
				TransportID: newTransports,
			},
		}
		pClusters = append(pClusters, pCluster)
	}

	finalVal := &protocol.Clusters{
		Cluster: pClusters,
	}

	out, err := proto.Marshal(finalVal)
	if err != nil {
		panic(err)
	}

	if err := os.WriteFile(clusterPath, out, 0644); err != nil {
		panic(err)
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
				return FixedMarginOverlap(times1, times2, 1e9)
			},
		}
		allTimestamps = append(allTimestamps, ts)
	}
	sort.Slice(allTimestamps, func(i, j int) bool {
		return allTimestamps[i].times[0] < allTimestamps[j].times[0] // sort by open time
	})

	// populate co-occurrence map
	cooc := NewCoOccurrence(nil)
	for _, ts := range allTimestamps {
		cooc.SetInnerKeys(ts.tid)
	}

	for _, ts1 := range allTimestamps {
		for _, ts2 := range allTimestamps {
			if ts1.times[len(ts1.times)-1]+2*1e9 < ts2.times[0] {
				break // no overlap further ahead since intervals are sorted by open time
			}
			cooc.CalcAndSet(ts1, ts2)
		}
	}
	cooc.SaveData(coOccurrencePath)

	// // print statistics about overlaps
	// float64Overlaps := make([]float64, 0)
	// for _, cmap := range *cooc.cMap {
	// 	for _, v := range cmap {
	// 		new_v := TsFloat(v) // convert to seconds
	// 		if new_v > 0 {
	// 			float64Overlaps = append(float64Overlaps, new_v)
	// 		}
	// 	}
	// }
	// fmt.Printf(`# of timestamps: %d

	// # non-zero overlaps: %d

	// 	Min: %.9f
	// 	Max: %.9f
	// 	Mean: %.9f
	// 	StdDev: %.9f

	// `,

	// 	len(allTimestamps),
	// 	len(float64Overlaps),
	// 	floats.Min(float64Overlaps),
	// 	floats.Max(float64Overlaps),
	// 	stat.Mean(float64Overlaps, nil),
	// 	stat.StdDev(float64Overlaps, nil),
	// )
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
