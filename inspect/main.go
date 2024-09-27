package main

import (
	"log"
	"os"
	"sort"

	"bringyour.com/inspect/data"
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

	sourceIP := "145.94.160.91"

	// dataPath := "data/capture.pcap"
	// savePath := "data/test_transports.pb"
	// coOccurrencePath := "data/cooccurrence_data_small.pb"

	// dataPath := "data/100k_tcp.pcap"
	// savePath := "data/test_2k_records.pb"
	// coOccurrencePath := "data/cooccurrence_data_2k.pb"

	dataPath := "data/test_session_1.pcapng"
	savePath := "data/ts1_transports.pb"
	coOccurrencePath := "data/ts1_cooccurrence.pb"

	switch fname {
	case "parse_pcap", "p":
		data.PcapToTransportFiles(dataPath, savePath, sourceIP)
	case "display_transports", "dt":
		data.DisplayTransports(savePath)
	case "timestamps", "t":
		makeTimestamps(savePath, coOccurrencePath)
	case "cluster", "c":
		cluster(coOccurrencePath)
	default:
		log.Fatalf("Unknown mode: %s", fname)
	}
}

func makeTimestamps(savePath string, coOccurrencePath string) {
	records, err := data.LoadTransportsFromFiles(savePath)
	if err != nil {
		log.Fatalf("Error loading transports: %v", err)
	}

	// go thorugh all records and find repeating server names in the open
	seenNames := make(map[string]bool)
	for _, record := range records {
		sName := *(record.Open.TlsServerName)
		if sName != "" {
			if _, ok := seenNames[sName]; ok {
				log.Printf("Repeating server name: %s", sName)
			}
			seenNames[sName] = true
		} else {
			log.Printf("Empty server name")
		}
	}
	// return

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
			sid:   sessionID(record.Open.TransportId),
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
	// populate inner maps to have all needed keys
	for _, ts := range allTimestamps {
		cooc.SetInnerKeys(ts.sid)
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

	// print statistics about overlaps
	float64Overlaps := make([]float64, 0)
	for _, cmap := range *cooc.cMap {
		for _, v := range cmap {
			new_v := TsFloat(v) // convert to seconds
			if new_v > 0 {
				float64Overlaps = append(float64Overlaps, new_v)
			}
		}
	}
	log.Printf(`
	# of timestamps: %d
	# non-zero overlaps: %d
		Min: %.9f
		Max: %.9f
		Mean: %.9f
		StdDev: %.9f`,

		len(allTimestamps),
		len(float64Overlaps),
		floats.Min(float64Overlaps),
		floats.Max(float64Overlaps),
		stat.Mean(float64Overlaps, nil),
		stat.StdDev(float64Overlaps, nil),
	)
}
