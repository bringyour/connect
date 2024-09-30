package main

import (
	"log"
	"os"

	"bringyour.com/inspect/data"
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
		idkLetsGo(savePath, coOccurrencePath)
	// case "cluster", "c":
	// 	cluster(coOccurrencePath)
	default:
		log.Fatalf("Unknown mode: %s", fname)
	}
}

func idkLetsGo(savePath string, coOccurrencePath string) {
	records, err := data.LoadTransportsFromFiles(savePath)
	if err != nil {
		log.Fatalf("Error loading transports: %v", err)
	}

	clusterMethod := NewOptics("min_samples->2,max_eps->0.9")
	// clusterMethod := NewHDBSCAN("min_cluster_size->5")

	score := makeTimestamps(records, coOccurrencePath, clusterMethod)
	log.Printf("Score: %f", score)
}
