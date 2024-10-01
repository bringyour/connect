package main

import (
	"fmt"
	"log"
	"os"

	"bringyour.com/inspect/data"
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

	switch fname {
	case "parse_pcap", "p":
		data.PcapToTransportFiles(dataPath, savePath, sourceIP)
	case "display_transports", "dt":
		data.DisplayTransports(savePath)
	case "timestamps", "t":
		testTimestamps(savePath, coOccurrencePath)
	case "cluster", "c":
		testCluster(coOccurrencePath)
	case "evaluate", "e":
		testEvaluate(savePath, coOccurrencePath)
	default:
		GeneticHillClimbing(savePath, coOccurrencePath)
		// GeneticOES(savePath, coOccurrencePath)
		// log.Fatalf("Unknown mode: %s", fname)
	}
}

func testTimestamps(savePath, coOccurrencePath string) {
	records, err := data.LoadTransportsFromFiles(savePath)
	if err != nil {
		log.Fatalf("Error loading transports: %v", err)
	}

	// build cooccurrence map
	sessionTimestamps := makeTimestamps(records)
	cooc, _ := makeCoOccurrence(sessionTimestamps)
	cooc.SaveData(coOccurrencePath)
	overlapStats(cooc)
}

func testCluster(coOccurrencePath string) {
	// cluster
	opticsOpts := fmt.Sprintf("min_samples=%d,max_eps=%f", 4, 0.9)
	clusterMethod := NewOptics(opticsOpts)
	// clusterMethod := NewHDBSCAN("min_cluster_size=5")
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

func testEvaluate(savePath string, coOccurrencePath string) {
	records, err := data.LoadTransportsFromFiles(savePath)
	if err != nil {
		log.Fatalf("Error loading transports: %v", err)
	}

	// build cooccurrence map
	sessionTimestamps := makeTimestamps(records)
	cooc, earliestTimestamp := makeCoOccurrence(sessionTimestamps)
	cooc.SaveData(coOccurrencePath)

	// cluster
	opticsOpts := fmt.Sprintf("min_samples=%d,max_eps=%f", 6, 0.92294)
	clusterMethod := NewOptics(opticsOpts)
	// hdbscanOpts := fmt.Sprintf("min_cluster_size=%d,cluster_selection_epsilon=%f", 2, 0.84466)
	// clusterMethod := NewHDBSCAN(hdbscanOpts)
	clusterOps := &ClusterOpts{
		ClusterMethod:    clusterMethod,
		CoOccurrencePath: coOccurrencePath,
		SaveGraphs:       true,
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

	// evaluate
	regions := ConstructTestSession1Regions(earliestTimestamp, 3)
	// for i, r := range regions {
	// 	fmt.Printf("Region %d: %s - %s\n", i+1, readableTime(r.minT), readableTime(r.maxT))
	// }
	score := Evaluate(*sessionTimestamps, *regions, clusters)
	log.Printf("Score: %f", score)
}
