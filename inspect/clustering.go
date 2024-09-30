package main

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"os/exec"
	"regexp"
	"sort"
	"strings"

	"bringyour.com/inspect/data"
	"github.com/oklog/ulid/v2"
)

func makeTimestamps(records map[ulid.ULID]*data.TransportRecord, coOccurrencePath string, clusterMethod ClusterMethod) float64 {
	// extract timestamps from records
	combinedTimestamps := make(map[sessionID]*timestamps, 0)
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

		sid := sessionID(*record.Open.TlsServerName)

		if ts, exists := combinedTimestamps[sid]; exists {
			ts.times = append(ts.times, times...)
		} else {
			ts := &timestamps{
				sid:   sid,
				times: times,
				overlapFunc: func(times1 []uint64, times2 []uint64) uint64 {
					return FixedMarginOverlap(times1, times2, 1e9)
				},
			}
			combinedTimestamps[sid] = ts
		}
	}
	// sort the timestamps of each session
	for _, ts := range combinedTimestamps {
		sort.Slice(ts.times, func(i, j int) bool {
			return ts.times[i] < ts.times[j]
		})
	}

	allTimestamps := make([]*timestamps, 0)
	for _, ts := range combinedTimestamps {
		allTimestamps = append(allTimestamps, ts)
	}
	// sort by open time for each session
	sort.Slice(allTimestamps, func(i, j int) bool {
		return allTimestamps[i].times[0] < allTimestamps[j].times[0]
	})

	// populate co-occurrence map
	cooc := NewCoOccurrence(nil)
	for _, ts := range allTimestamps {
		cooc.SetInnerKeys(ts.sid) // populate inner maps to have all needed keys
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
	// log.Printf(`
	// # of timestamps: %d
	// # non-zero overlaps: %d
	// 	Min: %.9f
	// 	Max: %.9f
	// 	Mean: %.9f
	// 	StdDev: %.9f`,

	// 	len(allTimestamps),
	// 	len(float64Overlaps),
	// 	floats.Min(float64Overlaps),
	// 	floats.Max(float64Overlaps),
	// 	stat.Mean(float64Overlaps, nil),
	// 	stat.StdDev(float64Overlaps, nil),
	// )

	clusterOps := &ClusterOpts{
		ClusterMethod:    clusterMethod,
		CoOccurrencePath: coOccurrencePath,
		// SaveGraphs:       true,
		// ShowHeatmapStats: true,
	}

	clusters, err := cluster(clusterOps)
	if err != nil {
		log.Fatalf("Error clustering: %v", err)
	}

	// fmt.Println(readableTime(allTimestamps[0].times[0]))
	regions := constructRegions(allTimestamps[0].times[0], 0)
	for i, r := range regions {
		fmt.Printf("Region %d: %s - %s\n", i+1, readableTime(r.minT), readableTime(r.maxT))
	}

	purities := make([]float64, 0)

	// print the clusters
	for clusterID, sessionIDs := range clusters {
		fmt.Printf("Cluster %s (%d):\n", clusterID, len(sessionIDs))
		regionCounts := make(map[int]int)
		regionCounts[-1] = 0
		for _, sid := range sessionIDs {
			if timestamps, exists := combinedTimestamps[sid]; exists {
				region := getSessionRegion(timestamps.times[0], regions)
				fmt.Printf("  [%s](%v)%v\n", sid, readableTime(timestamps.times[0]), region)
				// check if region is int
				if isInt := region == math.Round(region); isInt {
					if _, exists := regionCounts[int(region)]; !exists {
						regionCounts[int(region)] = 0
					}
					regionCounts[int(region)]++
				} else {
					// if region is not int, then it should be unclustered so we ignore
					regionCounts[-1]++
				}
			} else {
				log.Fatalf("Session ID %s not found in timestamps", sid)
			}
		}

		maxCount := 0
		if clusterID == "-1" {
			maxCount = regionCounts[-1]
		} else {
			for region, count := range regionCounts {
				if region == -1 {
					continue
				}
				if count > maxCount {
					maxCount = count
				}
			}
		}
		// do not count points that should be unclustered towards purity in non-unclustered clusters
		unclusteredTotal := len(sessionIDs) - regionCounts[-1]
		fmt.Printf("  Cluster has purity: %v/%v\n", maxCount, unclusteredTotal)
		purity := 1.0
		if unclusteredTotal != 0 {
			purity = float64(maxCount) / float64(unclusteredTotal)
		}
		purities = append(purities, purity)
		fmt.Println()
	}
	// do we consider the -1 cluster for purities
	// how do we weigh in on number of clusters

	expectedRegions := len(regions)
	formedClusters := len(clusters) - 1 // -1 for the unclustered cluster
	// size metric is exp(-|formedClusters - expectedRegions| / expectedRegions)
	clusterDiff := math.Abs(float64(formedClusters - expectedRegions))
	sizeRatio := clusterDiff / float64(expectedRegions)
	sizeMetric := math.Exp(-sizeRatio)
	// fmt.Printf("Size Metric: %v\n", sizeMetric)

	totalPurity := 0.0
	for _, purity := range purities {
		totalPurity += purity
	}
	averagePurity := totalPurity / float64(len(purities))
	// fmt.Println(averagePurity)

	score := 0.5*sizeMetric + averagePurity*0.5
	// fmt.Printf("Score: %v\n", score)
	return score
}

type ClusterMethod interface {
	Name() string
	Args() string
}

type ClusterOpts struct {
	ClusterMethod
	CoOccurrencePath string
	SaveGraphs       bool
	ShowHeatmapStats bool
}

func (co *ClusterOpts) GetFormatted() []string {
	args := []string{}
	args = append(args, "filename=../"+co.CoOccurrencePath)
	if co.SaveGraphs {
		graphs := "show_graph="
		if co.ShowHeatmapStats {
			graphs += "print_stats"
		} else {
			graphs += "no_stats"
		}
		args = append(args, graphs)
	}
	args = append(args, "cluster_method="+co.Name())
	args = append(args, "cluster_args="+co.Args())
	return args
}

type generalClusterMethod struct {
	name string
	args string
}

func NewOptics(clusterArgs string) ClusterMethod {
	return &generalClusterMethod{
		name: "OPTICS",
		args: clusterArgs,
	}
}

func NewHDBSCAN(clusterArgs string) ClusterMethod {
	return &generalClusterMethod{
		name: "HDBSCAN",
		args: clusterArgs,
	}
}

func (o *generalClusterMethod) Name() string {
	return o.name
}

func (o *generalClusterMethod) Args() string {
	return o.args
}

func cluster(clusterOps *ClusterOpts) (map[string][]sessionID, error) {
	args := []string{"main.py"}
	args = append(args, clusterOps.GetFormatted()...)
	// cmd: python3 main.py clusterArgs...
	cmd := exec.Command("python3", args...)
	cmd.Dir = "./analysis"
	var stderr bytes.Buffer
	var stdout bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdout = &stdout
	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("python script: %v %v", err, stderr.String())
	}

	clusters := make(map[string][]sessionID)
	lines := strings.Split(stdout.String(), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "[cluster]") {
			// extract cluster ID and session IDs
			re := regexp.MustCompile(`\[cluster\](-?\d+):\s*\[(.*?)\]`) // match [cluster]123: [sid1, sid2, sid3]
			matches := re.FindStringSubmatch(line)

			if len(matches) == 3 {
				clusterID := matches[1]
				sessionIDs := strings.Split(matches[2], ",")
				finalSids := make([]sessionID, len(sessionIDs))
				// trim spaces and single quotes from session IDs
				for i := range sessionIDs {
					finalSids[i] = sessionID(strings.Trim(sessionIDs[i], " '"))
				}
				clusters[clusterID] = finalSids
			} else {
				log.Printf("Error parsing cluster line: %s", line)
			}
			continue // skip to the next line
		}

		fmt.Printf("[py] %s\n", line) // print lines without the specified prefixes
	}

	// 	// print the clusters
	// 	for clusterID, sessionIDs := range clusters {
	// 		fmt.Printf("Cluster %s: %v\n", clusterID, sessionIDs)
	// 	}

	return clusters, nil
}
