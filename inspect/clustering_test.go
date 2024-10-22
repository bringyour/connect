package main

import (
	"fmt"
	"os"
	"testing"

	"bringyour.com/inspect/data"
)

// load pcaps into transport records
// make coocurrence map and cluster based on some params (how do i choose these params for the tests?)
// for coocurrence assume for now that there is an optimal overlapFunc so it is not a parameter
// cluster based on chosen params
// evaluate the clusters based on predefined clusters

// make help function for listing all sids in pcap

type clusterTest struct {
	name string
	sids []sessionID
}

func TestClustering(t *testing.T) {
	testFolder := "data/test_cluster/"

	tests := []struct {
		name     string
		pcapPath string
		sourceIP string
		clusters []clusterTest
	}{
		{
			name:     "Test Session 1",
			pcapPath: "data/ts1/ts1.pcapng",
			sourceIP: "145.94.160.91",
			clusters: []clusterTest{
				{
					name: "New York Times",
					sids: []sessionID{
						"www.nytimes.com",
						"static01.nytimes.com",
						"purr.nytimes.com",
						"nytimes.com",
						"nyt.com",
						"g1.nyt.com",
						"a.nytimes.com",
					},
				},
				{
					name: "Wikipedia",
					sids: []sessionID{
						"wikipedia.org",
						"wikimedia.org",
						"upload.wikimedia.org",
						"en.wikipedia.org",
					},
				},
				{
					name: "Whatsapp",
					sids: []sessionID{
						"mmg.whatsapp.net",
						"media-lhr8-2.cdn.whatsapp.net",
						"media-lhr8-1.cdn.whatsapp.net",
						"media-lhr6-2.cdn.whatsapp.net",
						"media-lhr6-1.cdn.whatsapp.net",
						"media-ams4-1.cdn.whatsapp.net",
						"media-ams2-1.cdn.whatsapp.net",
					},
				},
				{
					name: "CAT",
					sids: []sessionID{
						"www.cat.com",
						"scene7.com",
						"s7d2.scene7.com",
					},
				},
				{
					name: "Netflix",
					sids: []sessionID{
						"www.netflix.com",
						"web.prod.cloud.netflix.com",
						"occ-0-768-769.1.nflxso.net",
						"nflxvideo.net",
						"nflxso.net",
						"netflix.com",
					},
				},
			},
		},
		{
			name:     "Test Session 2",
			pcapPath: "data/ts2/ts2.pcapng",
			sourceIP: "145.94.190.27",
			clusters: []clusterTest{
				{
					name: "New York Times",
					sids: []sessionID{
						"www.nytimes.com",
						"purr.nytimes.com",
						"nytimes.com",
						"nyt.com",
						"g1.nyt.com",
						"a.nytimes.com",
					},
				},
				{
					name: "Twitter",
					sids: []sessionID{
						"x.com",
						"video.twimg.com",
						"twimg.com",
						"pbs.twimg.com",
						"api.x.com",
					},
				},
				{
					name: "Netflix",
					sids: []sessionID{
						"www.netflix.com",
						"web.prod.cloud.netflix.com",
						"occ-0-768-769.1.nflxso.net",
						"nflxvideo.net",
						"nflxso.net",
						"nflxext.com",
						"netflix.com",
						"ipv4-c197-ams001-ix.1.oca.nflxvideo.net",
						"ipv4-c195-ams001-ix.1.oca.nflxvideo.net",
						"ipv4-c186-ams001-ix.1.oca.nflxvideo.net",
						"ipv4-c177-ams001-ix.1.oca.nflxvideo.net",
						"assets.nflxext.com",
					},
				},
				// {
				// 	name: "NBC",
				// 	sids: []sessionID{
				// 		"nbc.com",
				// 		"img.nbc.com",
				// 	},
				// },
				{
					name: "Fox News",
					sids: []sessionID{
						"www.foxnews.com",
						"static.foxnews.com",
						"foxnews.com",
						"a57.foxnews.com",
					},
				},
				{
					name: "Reddit",
					sids: []sessionID{
						"www.reddit.com",
						"styles.redditmedia.com",
						"redditmedia.com",
						"redd.it",
						"preview.redd.it",
					},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cooccurrencePath := testFolder + test.name + " cooccurrence.pb"

			if _, err := os.Stat(cooccurrencePath); err == nil {
				// use existing cooccurrence map
				fmt.Printf("Using existing cooccurrence map in %q\n", cooccurrencePath)
			} else {
				transportsPath := testFolder + test.name + " transports.pb"
				if _, err := os.Stat(transportsPath); err == nil {
					// use existing transport records
					fmt.Printf("Using existing transport records in %q\n", transportsPath)
				} else {
					// create transport records from pcap
					fmt.Printf("Creating transport records from pcap: %s\n", test.pcapPath)
					data.PcapToTransportFiles(test.pcapPath, transportsPath, test.sourceIP)
				}

				fmt.Printf("Creating cooccurrence map from transport records: %s\n", transportsPath)

				// create timestamps for cooccurance map
				records, err := data.LoadTransportsFromFiles(transportsPath)
				if err != nil {
					t.Fatalf("Error loading transports: %v", err)
				}
				overlapFunctions := GaussianOverlap{
					stdDev: TimestampInNano(0.010_000_000), // x seconds
					cutoff: 4,                              // x standard deviations
				}
				sessionTimestamps := makeTimestamps(&overlapFunctions, records)

				// make coocurrence map
				cooc, _ := makeCoOccurrence(sessionTimestamps)
				cooc.SaveData(cooccurrencePath)
			}

			// cluster
			hdbscanOpts := fmt.Sprintf("min_cluster_size=%d,min_samples=%d,cluster_selection_epsilon=%.12f,alpha=%.12f", 4, 1, 0.001, 0.001)
			clusterMethod := NewHDBSCAN(hdbscanOpts)
			clusterOps := &ClusterOpts{
				ClusterMethod:    clusterMethod,
				CoOccurrencePath: cooccurrencePath,
				// SaveGraphs:       true,
			}
			clusters, _, err := cluster(clusterOps, true)
			if err != nil {
				t.Fatalf("Error clustering: %v", err)
			}

			// evaluate
			for _, correctCluster := range test.clusters {
				t.Run(correctCluster.name, func(t *testing.T) {
					clusterIds := []string{} // cluster ids of correct cluster
					expectedId := "unknown"
					// check if all sids are in the same cluster
					for _, correctSid := range correctCluster.sids {
						for i, cluster := range clusters {
							for _, sid := range cluster {
								if sid == correctSid {
									clusterIds = append(clusterIds, fmt.Sprintf("%s=%s", sid, i))
									if expectedId == "unknown" {
										expectedId = i // expect all sids to be in the same cluster
									} else if expectedId != i {
										expectedId = "" // not all sids are in the same cluster
									}
								}
							}
						}
					}
					if expectedId == "" || expectedId == "-1" {
						t.Fatalf("Cluster %q not formed correctly (%v)", correctCluster.name, clusterIds)
					}
					t.Logf("Cluster %q formed correctly (all sids are in cluster %s)\n", correctCluster.name, expectedId)
				})
			}
		})
	}
}
