package main

import (
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

func readableTime(timestamp uint64) string {
	return time.Unix(0, int64(timestamp)).Format("2006-01-02 15:04:05.000000")
}

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

func constructRegions(earliestTime uint64, leeway uint64) []region {
	// 0-11 search for dog
	// 11-69 browse dog webpage
	// 69-78 search for cat
	// 78-136 browser through cat
	// 136-185 netflix
	// 185-194 search for ny times
	// 194-250 browse nytimes
	// search should not be included
	firstRegions := []region{
		{minT: 11, maxT: 69},
		{minT: 78, maxT: 136},
		{minT: 136, maxT: 185},
		{minT: 194, maxT: 250},
	}

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
		newMinT := r.minT*1e9 + earliestTime
		newMaxT := r.maxT*1e9 + earliestTime
		newRegiond := region{minT: newMinT, maxT: newMaxT}
		finalRegions = append(finalRegions, newRegiond)
	}

	return finalRegions
}
