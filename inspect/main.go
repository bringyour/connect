package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"bringyour.com/inspect/evaluation"
	"bringyour.com/inspect/grouping"
	"bringyour.com/inspect/payload"
	"bringyour.com/protocol"

	"github.com/oklog/ulid/v2"
	"gonum.org/v1/gonum/floats"
	"gonum.org/v1/gonum/stat"
	"google.golang.org/protobuf/proto"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Usage: %s {p,dt,t,c,e,ghc,st}\n", os.Args[0])
	}
	fname := os.Args[1]

	// File paths for original data, transport records and cooccurence matrix
	// testCase := evaluation.TestSession1
	testCase := evaluation.TestSession2
	fmt.Printf("Test Case=%+v\n", testCase)

	// CLUSTERING OPTIONS
	// opticsOpts := fmt.Sprintf("min_samples=%d,max_eps=%f", 3, 0.20227)
	// clusterMethod := grouping.NewOptics(opticsOpts)
	hdbscanOpts := fmt.Sprintf("min_cluster_size=%d,min_samples=%d,cluster_selection_epsilon=%.12f,alpha=%.12f", 4, 1, 0.001, 0.001)
	clusterMethod := grouping.NewHDBSCAN(hdbscanOpts)

	// OVERLAP FUNCTIONS
	// overlapFunctions := grouping.FixedMarginOverlap{
	// 	Margin: grouping.TimestampInNano(0.010_000_000), // x seconds fixed margin
	// }
	overlapFunctions := grouping.GaussianOverlap{
		StdDev: grouping.TimestampInNano(0.010_000_000), // x seconds
		Cutoff: 4,                                       // x standard deviations
	}

	if fname == "parse_pcap" || fname == "p" {
		payload.PcapToTransportFiles(testCase.DataPath, testCase.SavePath, testCase.SourceIP)
	} else {
		records, err := payload.LoadTransportsFromFiles(testCase.SavePath)
		if err != nil {
			log.Fatalf("Error loading transports: %v", err)
		}

		if fname == "display_transports" || fname == "dt" {
			payload.DisplayTransports(records)
		} else if fname == "timestamps" || fname == "t" {
			parseTimestamps(&overlapFunctions, records, testCase.CoOccurrencePath)
		} else if fname == "cluster" || fname == "c" {
			runCluster(clusterMethod, testCase.CoOccurrencePath)
		} else if fname == "evaluate" || fname == "e" {
			runEvaluate(&overlapFunctions, clusterMethod, records, testCase.CoOccurrencePath, testCase.RegionsFunc)
		} else if fname == "genetic_hill_climbing" || fname == "ghc" {
			evaluation.GeneticHillClimbing(records, testCase)
		} else if fname == "save_times" || fname == "st" {
			saveTimes(&overlapFunctions, records, testCase.TimesPath)
		} else {
			log.Fatalf("Unknown mode: %s", fname)
		}
	}
}

func parseTimestamps(overlapFunctions grouping.OverlapFunctions, records *map[ulid.ULID]*payload.TransportRecord, coOccurrencePath string) {
	// build cooccurrence map
	sessionTimestamps := grouping.MakeTimestamps(overlapFunctions, records)
	cooc, _ := grouping.MakeCoOccurrence(sessionTimestamps)
	cooc.SaveData(coOccurrencePath)
	overlapStats(cooc)
}

func runCluster(clusterMethod grouping.ClusterMethod, coOccurrencePath string) {
	// cluster
	clusterOps := &grouping.ClusterOpts{
		ClusterMethod:    clusterMethod,
		CoOccurrencePath: coOccurrencePath,
		SaveGraphs:       true,
		ShowHeatmapStats: true,
	}
	clusters, probabilities, err := grouping.Cluster(clusterOps, true)
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

func runEvaluate(overlapFunctions grouping.OverlapFunctions, clusterMethod grouping.ClusterMethod, records *map[ulid.ULID]*payload.TransportRecord, coOccurrencePath string, regionsFunc evaluation.RegionsFunc) {
	time1 := time.Now()
	// build cooccurrence map
	sessionTimestamps := grouping.MakeTimestamps(overlapFunctions, records)
	cooc, earliestTimestamp := grouping.MakeCoOccurrence(sessionTimestamps)
	cooc.SaveData(coOccurrencePath)
	time1end := time.Since(time1)

	// cluster
	clusterOps := &grouping.ClusterOpts{
		ClusterMethod:    clusterMethod,
		CoOccurrencePath: coOccurrencePath,
		SaveGraphs:       true,
	}
	time2 := time.Now()
	clusters, probabilities, err := grouping.Cluster(clusterOps, true)
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
	regions := regionsFunc(earliestTimestamp, 3)
	// for i, r := range *regions {
	// 	fmt.Printf("Region %d: %s - %s\n", i+1, ReadableTime(r.minT), ReadableTime(r.maxT))
	// }
	score := evaluation.Evaluate(*sessionTimestamps, *regions, clusters, probabilities)
	time3end := time.Since(time3)
	log.Printf("Score: %f", score)

	fmt.Printf("Time to build cooccurrence map: %v\n", time1end)
	fmt.Printf("Time to cluster(+heatmap): %v\n", time2end)
	fmt.Printf("Time to evaluate: %v\n", time3end)
}

func saveTimes(overlapFunctions grouping.OverlapFunctions, records *map[ulid.ULID]*payload.TransportRecord, timesPath string) {
	// build cooccurrence map
	sessionTimestamps := grouping.MakeTimestamps(overlapFunctions, records)

	timesData := make([]*protocol.Times, 0)
	for sid, timestamps := range *sessionTimestamps {
		times := protocol.Times{
			Sid:  string(sid),
			Time: timestamps.Times,
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

// print statistics about overlaps in cooccurrence map
func overlapStats(cooc *grouping.CoOccurrence) {
	float64Overlaps := make([]float64, 0)
	for _, cmap := range *cooc.Data {
		for _, v := range cmap {
			new_v := grouping.TimestampInSeconds(v)
			if new_v > 0 {
				float64Overlaps = append(float64Overlaps, new_v)
			}
		}
	}
	log.Printf(`Co-occurrence statistics:
# of timestamps: %d
# non-zero overlaps: %d
	Min: %.9f
	Max: %.9f
	Mean: %.9f
	StdDev: %.9f`,

		len(*cooc.Data),
		len(float64Overlaps),
		floats.Min(float64Overlaps),
		floats.Max(float64Overlaps),
		stat.Mean(float64Overlaps, nil),
		stat.StdDev(float64Overlaps, nil),
	)
}
