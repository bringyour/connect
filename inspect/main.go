package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"bringyour.com/inspect/data"
	"bringyour.com/protocol"
	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/proto"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Usage: %s INSPECT_MODE\n", os.Args[0])
	}
	fname := os.Args[1]

	// sourceIP := "145.94.160.91"
	sourceIP := "145.94.190.27" // needs to be changed based on the pcap

	// File paths for original data, transport records and cooccurence matrix
	dataPath := "test_data/test_session_2.pcapng"
	savePath := "test_data/ts2_transports.pb"
	coOccurrencePath := "test_data/ts2_cooccurrence.pb"
	timesPath := "test_data/ts2_times.pb"

	// CLUSTERING OPTIONS
	// opticsOpts := fmt.Sprintf("min_samples=%d,max_eps=%f", 3, 0.20227)
	// clusterMethod := NewOptics(opticsOpts)
	hdbscanOpts := fmt.Sprintf("min_cluster_size=%d,cluster_selection_epsilon=%f", 2, 0.04774)
	clusterMethod := NewHDBSCAN(hdbscanOpts)

	// OVERLAP FUNCTIONS
	overlapFunctions := FixedMarginOverlap{
		margin: TimestampInNano(0.010000000), // x seconds fixed margin
	}
	// overlapFunctions := GaussianOverlap{
	// 	stdDev: TimestampInNano(0.010000000), // x seconds
	// 	cutoff: 4,                            // x standard deviations
	// }

	if fname == "parse_pcap" || fname == "p" {
		data.PcapToTransportFiles(dataPath, savePath, sourceIP)
	} else {
		records, err := data.LoadTransportsFromFiles(savePath)
		if err != nil {
			log.Fatalf("Error loading transports: %v", err)
		}

		if fname == "display_transports" || fname == "dt" {
			data.DisplayTransports(records)
		} else if fname == "timestamps" || fname == "t" {
			testTimestamps(&overlapFunctions, records, coOccurrencePath)
		} else if fname == "cluster" || fname == "c" {
			testCluster(clusterMethod, coOccurrencePath)
		} else if fname == "evaluate" || fname == "e" {
			testEvaluate(&overlapFunctions, clusterMethod, records, coOccurrencePath)
		} else if fname == "genetic_hill_climbing" || fname == "ghc" {
			GeneticHillClimbing(records, coOccurrencePath)
		} else if fname == "save_times" || fname == "st" {
			saveTimes(&overlapFunctions, records, timesPath)
		} else {
			log.Fatalf("Unknown mode: %s", fname)
		}
	}
}

func testTimestamps(overlapFunctions OverlapFunctions, records *map[ulid.ULID]*data.TransportRecord, coOccurrencePath string) {
	// build cooccurrence map
	sessionTimestamps := makeTimestamps(overlapFunctions, records)
	cooc, _ := makeCoOccurrence(sessionTimestamps)
	cooc.SaveData(coOccurrencePath)
	overlapStats(cooc)
}

func testCluster(clusterMethod ClusterMethod, coOccurrencePath string) {
	// cluster
	clusterOps := &ClusterOpts{
		ClusterMethod:    clusterMethod,
		CoOccurrencePath: coOccurrencePath,
		SaveGraphs:       true,
		ShowHeatmapStats: true,
	}
	clusters, probabilities, err := cluster(clusterOps, true)
	if err != nil {
		log.Fatalf("Error clustering: %v", err)
	}

	for clusterID, probs := range probabilities {
		fmt.Printf("\nCluster %s (len=%d):\n", clusterID, len(clusters[clusterID]))
		avgProb := 0.0
		for i, sid := range clusters[clusterID] {
			fmt.Printf("  %s: %.3f\n", sid, probs[i])
			avgProb += probs[i]
		}
		if len(clusters[clusterID]) > 0 {
			avgProb /= float64(len(clusters[clusterID]))
			fmt.Printf("  Average probability: %.3f\n", avgProb)
		}
	}
}

func testEvaluate(overlapFunctions OverlapFunctions, clusterMethod ClusterMethod, records *map[ulid.ULID]*data.TransportRecord, coOccurrencePath string) {
	time1 := time.Now()
	// build cooccurrence map
	sessionTimestamps := makeTimestamps(overlapFunctions, records)
	cooc, earliestTimestamp := makeCoOccurrence(sessionTimestamps)
	cooc.SaveData(coOccurrencePath)
	time1end := time.Since(time1)

	// cluster
	clusterOps := &ClusterOpts{
		ClusterMethod:    clusterMethod,
		CoOccurrencePath: coOccurrencePath,
		SaveGraphs:       true,
	}
	time2 := time.Now()
	clusters, probabilities, err := cluster(clusterOps, true)
	if err != nil {
		log.Fatalf("Error clustering: %v", err)
	}
	time2end := time.Since(time2)
	for clusterID, probs := range probabilities {
		fmt.Printf("\nCluster %s (len=%d):\n", clusterID, len(clusters[clusterID]))
		avgProb := 0.0
		for i, sid := range clusters[clusterID] {
			fmt.Printf("  %s: %.3f\n", sid, probs[i])
			avgProb += probs[i]
		}
		if len(clusters[clusterID]) > 0 {
			avgProb /= float64(len(clusters[clusterID]))
			fmt.Printf("  Average probability: %.3f\n", avgProb)
		}
	}

	time3 := time.Now()
	// evaluate
	regions := ConstructTestSessionRegions(earliestTimestamp, 3)
	// for i, r := range regions {
	// 	fmt.Printf("Region %d: %s - %s\n", i+1, ReadableTime(r.minT), ReadableTime(r.maxT))
	// }
	score := Evaluate(*sessionTimestamps, *regions, clusters, probabilities)
	time3end := time.Since(time3)
	log.Printf("Score: %f", score)

	fmt.Printf("Time to build cooccurrence map: %v\n", time1end)
	fmt.Printf("Time to cluster(+heatmap): %v\n", time2end)
	fmt.Printf("Time to evaluate: %v\n", time3end)
}

func saveTimes(overlapFunctions OverlapFunctions, records *map[ulid.ULID]*data.TransportRecord, timesPath string) {
	// build cooccurrence map
	sessionTimestamps := makeTimestamps(overlapFunctions, records)

	timesData := make([]*protocol.Times, 0)
	for sid, timestamps := range *sessionTimestamps {
		times := protocol.Times{
			Sid:  string(sid),
			Time: timestamps.times,
		}
		timesData = append(timesData, &times)
	}

	dataToSave := &protocol.TimesData{
		Times: timesData,
	}

	out, err := proto.Marshal(dataToSave)
	if err != nil {
		panic(err)
	}

	if err := os.WriteFile(timesPath, out, 0644); err != nil {
		panic(err)
	}
}
