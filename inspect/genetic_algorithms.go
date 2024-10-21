package main

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"time"

	"bringyour.com/inspect/data"
	"github.com/MaxHalford/eaopt"
	"github.com/oklog/ulid/v2"
)

type GeneticCluster struct {
	minSamples uint64  // min_samples
	eps        float64 // max_eps
	stdDev     float64 // standard deviation
	scoreFunc  func(minSamples uint64, eps float64, stdDev float64) (float64, error)
}

// Evaluate evalutes a Bohachevsky function at the current coordinates.
func (c *GeneticCluster) Evaluate() (float64, error) {
	return c.scoreFunc(c.minSamples, c.eps, c.stdDev)
}

// Mutate replaces one of the current coordinates with a random value in [-100, -100].
func (c *GeneticCluster) Mutate(rng *rand.Rand) {
	method := rng.Intn(2)
	if method == 0 {
		// change minSamples
		change := uint64(rng.Intn(2) + 1) // 1 or 2
		if rng.Intn(2) == 0 {
			c.minSamples += change
		} else {
			c.minSamples -= change
		}
		c.minSamples = max(3, c.minSamples) // minSamples >= 3
	} else if method == 1 {
		change := rng.Float64() / 5.0 // range 0-0.2
		if rng.Intn(2) == 0 {
			c.eps += change
		} else {
			c.eps -= change
		}
		c.eps = math.Min(1, math.Max(0.1, c.eps)) // range 0.1-1
	} else {
		change := rng.Float64() / 3333.0 // range 0-0.0003
		if rng.Intn(2) == 0 {
			c.stdDev += change
		} else {
			c.stdDev -= change
		}
		c.stdDev = math.Max(0.00001, c.stdDev) // range 0.0001
	}
}

// Crossover does nothing.  It is defined only so *Coord2D implements the eaopt.Genome interface.
func (c *GeneticCluster) Crossover(other eaopt.Genome, rng *rand.Rand) {}

// Clone returns a copy of a *Coord2D.
func (c *GeneticCluster) Clone() eaopt.Genome {
	return &GeneticCluster{minSamples: c.minSamples, eps: c.eps, stdDev: c.stdDev, scoreFunc: c.scoreFunc}
}

func GeneticHillClimbing(records *map[ulid.ULID]*data.TransportRecord, coOccurrencePath string) {
	// use fixed margin overlap to calculate overlap
	// overlapFunctions := FixedMarginOverlap{
	// 	margin: 5 * NS_IN_SEC, // 1 second fixed margin
	// }
	overlapFunctions := GaussianOverlap{
		stdDev: TimestampInNano(0.010_000_000), // x seconds
		cutoff: 4,                              // x standard deviations
	}

	time1 := time.Now()
	// build cooccurrence map
	sessionTimestamps := makeTimestamps(&overlapFunctions, records)
	cooc, earliestTimestamp := makeCoOccurrence(sessionTimestamps)
	cooc.SaveData(coOccurrencePath)
	fmt.Printf("Cooccurrence took %v\n", time.Since(time1))

	regionLeeway := uint64(3)
	regions := constructTestSessionRegions(earliestTimestamp, regionLeeway)

	customScoreFunc := func(minSamples uint64, eps float64, stdDev float64) (float64, error) {
		path := fmt.Sprintf("data/ghc/cooc_%f.pb", stdDev)
		var cooc *coOccurrence
		time2 := time.Now()

		// check if there is a file already by the name in path
		if _, err := os.Stat(path); err == nil {
			fmt.Printf("File already exists: %s\n", path)
			cooc = NewCoOccurrence(nil)
			if err := cooc.LoadData(path); err != nil {
				panic(err)
			}
		} else if os.IsNotExist(err) {
			ovF := GaussianOverlap{
				stdDev: TimestampInNano(stdDev), // x seconds
				cutoff: 4,                       // x standard deviations
			}
			for _, ts := range *sessionTimestamps {
				ts.overlapFuncts = &ovF
			}
			cooc, _ = makeCoOccurrence(sessionTimestamps)
			if err := cooc.SaveData(path); err != nil {
				fmt.Println(err)
			}
		} else {
			// Some other error occurred
			panic(err)
		}

		// opticsOpts := fmt.Sprintf("min_samples=%d,max_eps=%f", minSamples, eps)
		// clusterMethod := NewOptics(opticsOpts)
		hdbscanOpts := fmt.Sprintf("min_cluster_size=%d,cluster_selection_epsilon=%f", minSamples, eps)
		clusterMethod := NewHDBSCAN(hdbscanOpts)

		clusterOps := &ClusterOpts{
			ClusterMethod:    clusterMethod,
			CoOccurrencePath: path,
		}
		clusters, probabilities, err := cluster(clusterOps, false)
		if err != nil {
			return 0, fmt.Errorf("error clustering: %v", err)
		}
		fmt.Printf("ScoreFunc took %v\n", time.Since(time2))
		fmt.Printf("%v:%v,%v ", clusterMethod.Name(), clusterMethod.Args(), stdDev)
		score := Evaluate(*sessionTimestamps, *regions, clusters, probabilities)
		return -1 * score, nil
	}

	// hill climbing is implemented as a GA using the ModMutationOnly model
	cfg := eaopt.NewDefaultGAConfig()
	cfg.Model = eaopt.ModMutationOnly{Strict: false}
	cfg.PopSize = 2
	cfg.ParallelEval = true
	cfg.NGenerations = 20

	// add a custom callback function to track progress
	minFit := math.MaxFloat64
	cfg.Callback = func(ga *eaopt.GA) {
		hof := ga.HallOfFame[0]
		fit := hof.Fitness
		if fit == minFit {
			return // output only when we make an improvement
		}
		best := hof.Genome.(*GeneticCluster)
		fmt.Printf("Best fitness at generation %4d: %10.5f at (%d, %9.5f, %.5f)\n",
			ga.Generations, fit, best.minSamples, best.eps, best.stdDev)
		minFit = fit
	}

	// run the hill-climbing algorithm
	ga, err := cfg.NewGA()
	ga.ParallelEval = true
	if err != nil {
		panic(err)
	}
	i := 0
	GetI := func() int {
		i++
		return i - 1
	}
	err = ga.Minimize(func(rng *rand.Rand) eaopt.Genome {
		if GetI() == 0 {
			return &GeneticCluster{
				minSamples: 4,
				eps:        0.1,
				stdDev:     0.010_000_000,
				scoreFunc:  customScoreFunc,
			}
		}
		return &GeneticCluster{
			minSamples: uint64(rng.Intn(7) + 3), // range: 3-10
			eps:        rng.Float64(),
			stdDev:     0.010_000_000, // rng.Float64(),
			scoreFunc:  customScoreFunc,
		}
	})
	if err != nil {
		panic(err)
	}

	// output the best encountered solution.
	best := ga.HallOfFame[0].Genome.(*GeneticCluster)
	bestScore := ga.HallOfFame[0].Fitness
	fmt.Printf("Found a minimum at (%d, %.5f, %.5f) with score %v.\n", best.minSamples, best.eps, best.stdDev, bestScore)
}
