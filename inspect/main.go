package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"bringyour.com/inspect/data"
	"github.com/oklog/ulid/v2"
)

func main() {
	// pcapFile := "data/1tls.pcapng"
	// pcapFile := "data/capture.pcap"
	// pcapFile := "data/client hellos.pcapng"

	if len(os.Args) != 2 {
		log.Fatalf("Usage: %s INSPECT_MODE\n", os.Args[0])
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

	// opticsOpts := fmt.Sprintf("min_samples=%d,max_eps=%f", 2, 0.2)
	// clusterMethod := NewOptics(opticsOpts)
	hdbscanOpts := fmt.Sprintf("min_cluster_size=%d,cluster_selection_epsilon=%f", 7, 0.20000)
	clusterMethod := NewHDBSCAN(hdbscanOpts)

	// avg metric HDBSCAN
	// !! (2, 0.40032, 0.44944)
	//
	// (7, 0.20000, 0.05000)
	// (2, 0.76745, 0.61240)

	// avg metric (no size) HDBSCAN
	// !!!! 7, 0.2, 0.0311724
	//
	// 9, 0.20000, 0.00201

	// use fixed margin overlap to calculate overlap
	// overlapFunctions := FixedMarginOverlap{
	// 	margin: 5 * NS_IN_SEC, // 1 second fixed margin
	// }
	overlapFunctions := GaussianOverlap{
		stdDev: TimestampInNano(0.0311724), // x seconds
		cutoff: 4,                          // x standard deviations
	}

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
		} else if fname == "genetic_oes" || fname == "goes" {
			GeneticOES(records, coOccurrencePath)
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
	clusters, err := cluster(clusterOps, true)
	if err != nil {
		log.Fatalf("Error clustering: %v", err)
	}

	for clusterID, sessionIDs := range clusters {
		fmt.Printf("\nCluster %s (len=%d):\n", clusterID, len(sessionIDs))
		for _, sid := range sessionIDs {
			fmt.Printf("  %s\n", sid)
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
	clusters, err := cluster(clusterOps, true)
	if err != nil {
		log.Fatalf("Error clustering: %v", err)
	}
	time2end := time.Since(time2)
	for clusterID, sessionIDs := range clusters {
		fmt.Printf("\nCluster %s (len=%d):\n", clusterID, len(sessionIDs))
		for _, sid := range sessionIDs {
			fmt.Printf("  %s\n", sid)
		}
	}

	time3 := time.Now()
	// evaluate
	regions := ConstructTestSession1Regions(earliestTimestamp, 3)
	// for i, r := range regions {
	// 	fmt.Printf("Region %d: %s - %s\n", i+1, readableTime(r.minT), readableTime(r.maxT))
	// }
	score := Evaluate(*sessionTimestamps, *regions, clusters)
	time3end := time.Since(time3)
	log.Printf("Score: %f", score)

	fmt.Printf("Time to build cooccurrence map: %v\n", time1end)
	fmt.Printf("Time to cluster(+heatmap): %v\n", time2end)
	fmt.Printf("Time to evaluate: %v\n", time3end)
}
