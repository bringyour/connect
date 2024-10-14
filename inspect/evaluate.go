package main

import (
	"fmt"
	"log"
	"math"
	"time"
)

type region struct {
	minT uint64
	maxT uint64
}

type clusteredSession struct {
	earliestTime uint64
	sessionID    string
	region       float64
}

func ReadableTime(timestamp uint64) string {
	return time.Unix(0, int64(timestamp)).Format("2006-01-02 15:04:05.000000")
}

// getSessionRegion returns the region ID of a session based on its earliest time
// if the session's earliest time is in between regions then the ID is the one inbetween the two corresponding regions
// i.e., if the session is in between region 1 and 2, then the ID is 1.5
func getSessionRegion(earliestTime uint64, regions []region) float64 {
	for i, r := range regions {
		regionID := i + 1
		if earliestTime < r.minT {
			// this session should have been in the previous half region
			return float64(regionID) - 0.5
		}
		if earliestTime <= r.maxT {
			// this session is in this region
			return float64(regionID)
		}
	}
	// this session is after the last region ends
	return float64(len(regions)) + 0.5
}

// constructRegions transforms the regions from relative times in seconds to absolute times (from the earliest time) in nanoseconds with added leeway
// firstRegions is the regions (with minT and maxT in seconds)
// earliestTime is the earliest time when data starts (in seconds)
// leeway is the added time in nanoseconds to the regions (if 0, then no leeway is added)
// returns the regions with minT and maxT in nanoseconds and added leeway
func constructRegions(firstRegions []region, earliestTime uint64, leeway uint64) *[]region {
	// add leeway to region bounds
	if leeway != 0 {
		for i, r := range firstRegions {
			if i == 0 {
				// first region
				firstRegions[i].minT = max(0, firstRegions[i].minT-leeway)
			} else if r.minT-leeway > firstRegions[i-1].maxT {
				// add leeway to current start
				firstRegions[i].minT -= leeway
			} else {
				// set current start to previous end
				firstRegions[i].minT = firstRegions[i-1].maxT
			}
			if i == len(firstRegions)-1 {
				// extend last region freely
				firstRegions[i].maxT = firstRegions[i].maxT + leeway
			} else if r.maxT+leeway < firstRegions[i+1].minT {
				// add leeway to current end if it doesn't overlap with next start
				firstRegions[i].maxT += leeway
			} else {
				// set current end to next start
				firstRegions[i].maxT = firstRegions[i+1].minT
			}
		}
	}

	finalRegions := []region{}
	for _, r := range firstRegions {
		// convert to nanoseconds and add earliest time
		newMinT := r.minT*NS_IN_SEC + earliestTime
		newMaxT := r.maxT*NS_IN_SEC + earliestTime
		newRegiond := region{minT: newMinT, maxT: newMaxT}
		finalRegions = append(finalRegions, newRegiond)
	}

	return &finalRegions
}

func ConstructTestSession1Regions(earliestTime uint64, leeway uint64) *[]region {
	firstRegions := []region{
		{minT: 11, maxT: 69},
		{minT: 78, maxT: 136},
		{minT: 136, maxT: 185},
		{minT: 194, maxT: 250},
	}
	finalRegions := constructRegions(firstRegions, earliestTime, leeway)
	return finalRegions
}

func Evaluate(sessionTimestamps map[sessionID]*timestamps, regions []region, clusters map[string][]sessionID) float64 {
	purities := make([]float64, 0)
	unclusteredPurity := 0.0

	for clusterID, sessionIDs := range clusters {
		// fmt.Printf("Cluster %s (%d):\n", clusterID, len(sessionIDs))

		// count the purity of the current cluster by counting the number of sessions in each expected region
		// and then extracting the biggest matching regions in the cluster as ratio of total sessions
		regionCounts := make(map[int]int)
		regionCounts[-1] = 0
		for _, sid := range sessionIDs {
			if timestamps, exists := sessionTimestamps[sid]; exists {
				sessionEarliestTime := timestamps.times[0]
				region := getSessionRegion(sessionEarliestTime, regions)
				// fmt.Printf("  [%s](%v)%v\n", sid, ReadableTime(timestamps.times[0]), region)
				// if expected region is int, then session is not unclustered (count towards purity)
				if isInt := region == math.Round(region); isInt {
					if _, exists := regionCounts[int(region)]; !exists {
						regionCounts[int(region)] = 0
					}
					regionCounts[int(region)]++
				} else { // if region is not int, then it should be unclustered so we add to unclustered count
					regionCounts[-1]++
				}
			} else {
				log.Fatalf("Session ID %s not found in timestamps", sid)
			}
		}

		if clusterID == "-1" {
			// unclustered cluster's purity is # of found unclustered sessions / total sessions
			unclusteredPurity = float64(regionCounts[-1]) / float64(len(sessionIDs))
			continue
		}

		// get region with most sessions assigned to it
		maxCount := 0
		for region, count := range regionCounts {
			if region == -1 {
				continue
			}
			if count > maxCount {
				maxCount = count
			}
		}
		// do not count points that should be unclustered towards purity in non-unclustered clusters
		unclusteredTotal := len(sessionIDs) - regionCounts[-1]
		// fmt.Printf("  Cluster has purity: %v/%v\n", maxCount, unclusteredTotal)

		// purity = most assigned region / total assigned regions (excluding unclustered)
		purity := 1.0
		if unclusteredTotal != 0 {
			purity = float64(maxCount) / float64(unclusteredTotal)
		}
		purities = append(purities, purity) // save purity for later metrics
		// fmt.Println()
	}

	// calculate average purity of clusters (excluding unclustered)
	averagePurity := 0.0
	if len(purities) > 0 {
		totalPurity := 0.0
		for _, purity := range purities {
			totalPurity += purity
		}
		averagePurity = totalPurity / float64(len(purities))
		// fmt.Println(averagePurity)
	}

	expectedRegions := len(regions)
	formedClusters := len(clusters) - 1 // exclude unclustered cluster

	// size metric is exp(-|formedClusters - expectedRegions| / expectedRegions)
	clusterDiff := math.Abs(float64(formedClusters - expectedRegions))
	sizeRatio := clusterDiff / float64(expectedRegions)
	sizeMetric := math.Exp(-sizeRatio)
	// fmt.Printf("Size Metric: %v\n", sizeMetric)

	// final score is average of size metric, average purity, and unclustered purity
	score := (sizeMetric + averagePurity + unclusteredPurity) / 3
	// score := (averagePurity + unclusteredPurity) / 2

	fmt.Printf("[%.5f] Clusters: %d, Size: %v, Purity: %v, Unclustered Purity: %v\n", score, len(clusters), sizeMetric, averagePurity, unclusteredPurity)
	return score
}
