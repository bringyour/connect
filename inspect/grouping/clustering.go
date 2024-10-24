package grouping

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"bringyour.com/inspect/payload"
	"bringyour.com/protocol"

	"github.com/humilityai/hdbscan"
	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/proto"
)

func MakeTimestamps(overlapFunctions OverlapFunctions, records *map[ulid.ULID]*payload.TransportRecord) *map[SessionID]*Timestamps {
	// extract for each session the timestamps of the open, write, read, and close records
	sessionTimestamps := make(map[SessionID]*Timestamps, 0)

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

		sid := SessionID(*record.Open.TlsServerName)
		var ts *Timestamps

		// add full domain to sessionTimestamps
		if ts1, exists := sessionTimestamps[sid]; exists {
			ts1.Times = append(ts1.Times, times...)
			ts = ts1
		} else {
			ts = &Timestamps{
				Sid:           sid,
				Times:         times,
				OverlapFuncts: overlapFunctions,
			}
			sessionTimestamps[sid] = ts
		}

		// splits ipv4-abc.1.api.bringyour.com into ipv4-abc.1, api.bringyour.com, bringyour.com, .net
		_, thirdLevelDomain, secondLevelDomain, _ := splitDomain(string(sid))
		// for now we ignore top level domains (like .com)

		// third level domain should not be "" or full domain
		if thirdLevelDomain != "" && thirdLevelDomain != string(sid) {
			if existingTimestamps, ok := sessionTimestamps[SessionID(thirdLevelDomain)]; ok {
				mergeTimestamps(existingTimestamps, ts)
			} else {
				sessionTimestamps[SessionID(thirdLevelDomain)] = &Timestamps{
					Sid:           SessionID(thirdLevelDomain),
					Times:         times,
					OverlapFuncts: overlapFunctions,
				}
			}
		}

		// second level domain should not be "" or full domain
		if secondLevelDomain != "" && secondLevelDomain != string(sid) {
			if existingTimestamps, ok := sessionTimestamps[SessionID(secondLevelDomain)]; ok {
				mergeTimestamps(existingTimestamps, ts)
			} else {
				sessionTimestamps[SessionID(secondLevelDomain)] = &Timestamps{
					Sid:           SessionID(secondLevelDomain),
					Times:         times,
					OverlapFuncts: overlapFunctions,
				}
			}
		}

		// if existingTimestamps, ok := sessionTimestamps[SessionID(thirdLevelDomain)]; ok {
		// 	mergeTimestamps(existingTimestamps, ts)
		// } else {
		// 	sessionTimestamps[SessionID(thirdLevelDomain)] = &timestamps{
		// 		sid:           SessionID(thirdLevelDomain),
		// 		times:         times,
		// 		overlapFuncts: overlapFunctions,
		// 	}
		// }
	}

	// sort the timestamps in each session (sorted by earliest open time)
	for _, ts := range sessionTimestamps {
		// fmt.Println(ts.sid, len(ts.times))
		sort.Slice(ts.Times, func(i, j int) bool {
			return ts.Times[i] < ts.Times[j]
		})
	}

	delete(sessionTimestamps, "")

	return &sessionTimestamps
}

func MakeCoOccurrence(sessionTimestamps *map[SessionID]*Timestamps) (*CoOccurrence, uint64) {
	// get all sessions' timestamps sorted by earliest open time
	allTimestamps := make([]*Timestamps, 0)
	for _, ts := range *sessionTimestamps {
		allTimestamps = append(allTimestamps, ts)
	}
	sort.Slice(allTimestamps, func(i, j int) bool {
		return allTimestamps[i].Times[0] < allTimestamps[j].Times[0] // sort based on first timestamp (earliest open time)
	})

	// populate co-occurrence map
	cooc := NewCoOccurrence(nil)
	for _, ts := range allTimestamps {
		cooc.SetOuterKey(ts.Sid) // populate outer map to have all needed keys
	}
	for _, ts1 := range allTimestamps {
		for _, ts2 := range allTimestamps {
			if ts1.NoFutureOverlap(ts2) {
				break // no overlap further ahead since intervals are sorted by open time
			}
			cooc.CalcAndSet(ts1, ts2)
		}
	}

	return cooc, allTimestamps[0].Times[0]
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

// overlapToDistance converts overlap to distance using exponential decay.
func overlapToDistance(overlap, maxOverlap float64) float64 {
	alpha := 13.0 // adjust alpha to control the rate of decay

	// no overlap means max distance
	if overlap <= 0 {
		return 1
	}

	// max overlap means no distance
	if overlap >= maxOverlap {
		return 0
	}

	// exponential decay
	return math.Exp(-alpha * (overlap / maxOverlap))
}

func Cluster(clusterOps *ClusterOpts, printPython bool) (map[string][]SessionID, map[string][]float64, error) {
	return PythonCluster(clusterOps, printPython)
}

func GoCluster(clusterOps *ClusterOpts, printPython bool) (map[string][]SessionID, map[string][]float64, error) {
	// load cooccurance map
	cooc := NewCoOccurrence(nil)
	err := cooc.LoadData(clusterOps.CoOccurrencePath)
	if err != nil {
		return nil, nil, fmt.Errorf("loading cooccurrence map: %v", err)
	}

	// get all sids (top level) in cooc map
	sids := make([]SessionID, 0)
	for sid := range *cooc.Data {
		sids = append(sids, sid)
	}
	// order sids lexicographically (for consistent results)
	sort.Slice(sids, func(i, j int) bool {
		return SessionID(sids[i]).Compare(SessionID(sids[j])) <= 0
	})

	// map sids to integers
	sidIndex := make(map[int]SessionID)
	index := 0
	for sid := range sids {
		sidIndex[index] = sids[sid]
		index++
	}

	// make array of the indices of the sids (as float64) for hdbscan data
	samples := make([][]float64, index)
	for i := 0; i < index; i++ {
		samples[i] = []float64{float64(i)}
	}

	// hdbscan
	minimumClusterSize := 3
	clustering, err := hdbscan.NewClustering(samples, minimumClusterSize)
	if err != nil {
		return nil, nil, fmt.Errorf("hdbscan clustering: %v", err)
	}

	// Options for clustering
	// --   chosen   --
	// [DEBUG] Verbose() - prints progress of clustering
	// [GOOD] OutlierDetection() - adds all unclustered as outliers (each is unique to its closest cluster, so taking all outliers is the unclustered points)
	// [INTERESTING] NearestNeighbor() - if used that outliers are assigned to clusters based on closest point in that cluster, otherwise based on centroid of cluster
	// -- not chosen --
	// [INTERESTING] OutlierClustering() - if the outliers of a cluster are more than minClusterSize then a new cluster is made with the outliers (most likely not useful)
	// [BAD] Voronoi() - makes sure everything is clustered (we dont want this in general)
	// [BAD] Subsample(n int) - only use first n points in clustering (bad for us since our points are sids)
	clustering = clustering.OutlierDetection().NearestNeighbor()

	distanceFunc := func(v1, v2 []float64) float64 {
		// get the sids from the indices
		sid1 := sidIndex[int(v1[0])]
		sid2 := sidIndex[int(v2[0])]
		// get the overlap value from the cooc map
		overlap := cooc.Get(sid1, sid2)
		// convert the overlap value to a distance
		maxOverlap := uint64(257004153827659)
		distance := overlapToDistance(TimestampInSeconds(overlap), TimestampInSeconds(maxOverlap))
		return distance
	}

	// run clustering
	score := hdbscan.VarianceScore // StabilityScore seems to not cluster anything so using VarianceScore
	minimumSpanningTree := false   // with true seems to cluster everything together so using false
	clustering.Run(distanceFunc, score, minimumSpanningTree)

	// fmt.Printf("%+v\n", clustering)
	// save results of clustering
	clusters := make(map[string][]SessionID)
	probabilities := make(map[string][]float64)
	clusters["-1"] = make([]SessionID, 0)    // unclustered
	probabilities["-1"] = make([]float64, 0) // unclustered
	for i, cluster := range clustering.Clusters {
		clusterID := fmt.Sprintf("%d", i)
		points := cluster.Points
		clusterSids := make([]SessionID, len(points))
		clusterProbs := make([]float64, len(points))
		for j, point := range points {
			clusterSids[j] = sidIndex[int(point)]
			clusterProbs[j] = 1.0 // set probabilites to 1.0
		}
		clusters[clusterID] = clusterSids
		probabilities[clusterID] = clusterProbs

		// add outliers as unclustered
		for _, outlier := range cluster.Outliers {
			clusters["-1"] = append(clusters["-1"], sidIndex[outlier.Index])
			probabilities["-1"] = append(probabilities["-1"], 0.0) // set probabilites to 0.0
		}
	}

	if clusterOps.SaveGraphs {
		// save clusters in file
		clustersPath := clusterOps.CoOccurrencePath + "_clusters.pb"
		if err := SaveClusters(clusters, clustersPath); err != nil {
			return nil, nil, fmt.Errorf("saving clusters: %v", err)
		}
		defer os.Remove(clustersPath) // delete clusters file

		// run python script to save graphs
		args := []string{"data_insights.py", "cooccurrence=../" + clusterOps.CoOccurrencePath, "clusters=../" + clustersPath}
		if clusterOps.ShowHeatmapStats {
			args = append(args, "print_stats=True")
		}
		if printPython {
			fmt.Printf("[cmd] python3 %s\n", strings.Join(args, " "))
		}
		cmd := exec.Command("python3", args...)
		cmd.Dir = "./analysis"
		var stderr bytes.Buffer
		var stdout bytes.Buffer
		cmd.Stderr = &stderr
		cmd.Stdout = &stdout
		err := cmd.Run()
		if err != nil {
			return nil, nil, fmt.Errorf("python script: %v %v", err, stderr.String())
		}

		if printPython {
			lines := strings.Split(stdout.String(), "\n")
			for _, line := range lines {
				if line == "" {
					continue
				}
				fmt.Printf("[py] %s\n", line) // print lines without the specified prefixes

			}
		}
	}

	return clusters, probabilities, nil
}

func PythonCluster(clusterOps *ClusterOpts, printPython bool) (map[string][]SessionID, map[string][]float64, error) {
	args := []string{"cluster.py"}
	if printPython {
		fmt.Printf("[cmd] python3 cluster.py %s\n", strings.Join(clusterOps.GetFormatted(), " "))
	}
	args = append(args, clusterOps.GetFormatted()...)
	// cmd: python3 cluster.py clusterArgs...
	cmd := exec.Command("python3", args...)
	cmd.Dir = "./analysis"
	var stderr bytes.Buffer
	var stdout bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdout = &stdout
	err := cmd.Run()
	if err != nil {
		return nil, nil, fmt.Errorf("python script: %v %v", err, stderr.String())
	}

	clusters := make(map[string][]SessionID)
	probabilities := make(map[string][]float64)
	lines := strings.Split(stdout.String(), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "[cluster]") {
			// extract cluster ID and session IDs
			re := regexp.MustCompile(`\[cluster\](-?\d+):\s*\[(.*?)\]\s*\[(.*?)]`) // match [cluster]123: [sid1, sid2, sid3] [prob1, prob2, prob3]
			matches := re.FindStringSubmatch(line)

			if len(matches) == 4 {
				clusterID := matches[1]
				sessionIDs := strings.Split(matches[2], ",")
				finalSids := make([]SessionID, len(sessionIDs))
				// trim spaces and single quotes from session IDs
				for i := range sessionIDs {
					finalSids[i] = SessionID(strings.Trim(sessionIDs[i], " '"))
				}
				clusters[clusterID] = finalSids

				// get probabilities as floats
				probs := strings.Split(matches[3], ",")
				clusterProbs := make([]float64, len(probs))
				for i, p := range probs {
					stringP := strings.Trim(p, " ")
					floatP, err := strconv.ParseFloat(stringP, 64)
					if err != nil {
						log.Fatalln("Error parsing probability:", err)
					} else {
						clusterProbs[i] = floatP
					}
				}
				probabilities[clusterID] = clusterProbs
			} else {
				log.Printf("Error parsing cluster line: %s", line)
				log.Println(matches)
			}
			continue // skip to the next line
		}
		if printPython {
			fmt.Printf("[py] %s\n", line) // print lines without the specified prefixes
		}
	}

	return clusters, probabilities, nil
}

func SaveClusters(clusters map[string][]SessionID, dataPath string) error {
	clustersData := make([]*protocol.Cluster, 0)
	for clusterID, sids := range clusters {
		stringSids := make([]string, len(sids))
		for i, sid := range sids {
			stringSids[i] = string(sid)
		}
		cluster := protocol.Cluster{
			Id:   clusterID,
			Sids: stringSids,
		}
		clustersData = append(clustersData, &cluster)
	}

	dataToSave := &protocol.ClustersData{
		Clusters: clustersData,
	}

	out, err := proto.Marshal(dataToSave)
	if err != nil {
		return err
	}

	return os.WriteFile(dataPath, out, 0644)
}
