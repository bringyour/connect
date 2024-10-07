package main

import (
	"bytes"
	"fmt"
	"log"
	"os/exec"
	"regexp"
	"sort"
	"strings"

	"bringyour.com/inspect/data"
	"github.com/oklog/ulid/v2"
	"gonum.org/v1/gonum/floats"
	"gonum.org/v1/gonum/stat"
)

func makeTimestamps(overlapFunctions OverlapFunctions, records *map[ulid.ULID]*data.TransportRecord) *map[sessionID]*timestamps {
	// extract for each session the timestamps of the open, write, read, and close records
	sessionTimestamps := make(map[sessionID]*timestamps, 0)

	for _, record := range *records {
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

		if ts, exists := sessionTimestamps[sid]; exists {
			ts.times = append(ts.times, times...)
		} else {
			ts := &timestamps{
				sid:           sid,
				times:         times,
				overlapFuncts: overlapFunctions,
			}
			sessionTimestamps[sid] = ts
		}
	}
	// sort the timestamps in each session (sorted by earliest open time)
	for _, ts := range sessionTimestamps {
		sort.Slice(ts.times, func(i, j int) bool {
			return ts.times[i] < ts.times[j]
		})
	}

	return &sessionTimestamps
}

func makeCoOccurrence(sessionTimestamps *map[sessionID]*timestamps) (*coOccurrence, uint64) {
	// get all sessions' timestamps sorted by earliest open time
	allTimestamps := make([]*timestamps, 0)
	for _, ts := range *sessionTimestamps {
		allTimestamps = append(allTimestamps, ts)
	}
	sort.Slice(allTimestamps, func(i, j int) bool {
		return allTimestamps[i].times[0] < allTimestamps[j].times[0] // sort based on first timestamp (earliest open time)
	})

	// populate co-occurrence map
	cooc := NewCoOccurrence(nil)
	for _, ts := range allTimestamps {
		// populate outer map to have all needed keys
		cooc.SetOuterKey(ts.sid)
	}
	for _, ts1 := range allTimestamps {
		for _, ts2 := range allTimestamps {
			if ts1.NoFutureOverlap(ts2) {
				break // no overlap further ahead since intervals are sorted by open time
			}
			cooc.CalcAndSet(ts1, ts2)
		}
	}

	return cooc, allTimestamps[0].times[0]
}

// print statistics about overlaps in cooccurrence map
func overlapStats(cooc *coOccurrence) {
	float64Overlaps := make([]float64, 0)
	for _, cmap := range *cooc.cMap {
		for _, v := range cmap {
			new_v := TimestampInSeconds(v)
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

		len(*cooc.cMap),
		len(float64Overlaps),
		floats.Min(float64Overlaps),
		floats.Max(float64Overlaps),
		stat.Mean(float64Overlaps, nil),
		stat.StdDev(float64Overlaps, nil),
	)
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

type ClusterMethod interface {
	Name() string
	Args() string
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

func cluster(clusterOps *ClusterOpts, printPython bool) (map[string][]sessionID, error) {
	args := []string{"main.py"}
	if printPython {
		fmt.Printf("[cmd] python3 main.py %s\n", strings.Join(clusterOps.GetFormatted(), " "))
	}
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
		if printPython {
			fmt.Printf("[py] %s\n", line) // print lines without the specified prefixes
		}
	}

	return clusters, nil
}
