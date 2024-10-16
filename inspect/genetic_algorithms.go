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

// A Coord2D is a coordinate in two dimensions.
type Coord3D struct {
	X         uint64  // min_samples
	Y         float64 // max_eps
	Z         float64 // standard deviation
	scoreFunc func(X uint64, Y float64, Z float64) (float64, error)
}

// Evaluate evalutes a Bohachevsky function at the current coordinates.
func (c *Coord3D) Evaluate() (float64, error) {
	return c.scoreFunc(c.X, c.Y, c.Z)
}

// Mutate replaces one of the current coordinates with a random value in [-100, -100].
func (c *Coord3D) Mutate(rng *rand.Rand) {
	method := rng.Intn(2)
	if method == 0 {
		// change X
		change := uint64(rng.Intn(2) + 1) // 1 or 2
		if rng.Intn(2) == 0 {
			c.X += change
		} else {
			c.X -= change
		}
		c.X = max(2, c.X) // X >= 2
	} else if method == 1 {
		change := rng.Float64() / 5.0 // range 0-0.2
		if rng.Intn(2) == 0 {
			c.Y += change
		} else {
			c.Y -= change
		}
		c.Y = math.Min(1, math.Max(0.1, c.Y)) // range 0.1-1
	} else {
		change := rng.Float64() / 33.0 // range 0-0.03
		if rng.Intn(2) == 0 {
			c.Z += change
		} else {
			c.Z -= change
		}
		c.Z = math.Max(0.0001, c.Z) // range 0.0001
	}
}

// Crossover does nothing.  It is defined only so *Coord2D implements the eaopt.Genome interface.
func (c *Coord3D) Crossover(other eaopt.Genome, rng *rand.Rand) {}

// Clone returns a copy of a *Coord2D.
func (c *Coord3D) Clone() eaopt.Genome {
	return &Coord3D{X: c.X, Y: c.Y, Z: c.Z, scoreFunc: c.scoreFunc}
}

func GeneticHillClimbing(records *map[ulid.ULID]*data.TransportRecord, coOccurrencePath string) {
	// use fixed margin overlap to calculate overlap
	// overlapFunctions := FixedMarginOverlap{
	// 	margin: 5 * NS_IN_SEC, // 1 second fixed margin
	// }
	overlapFunctions := GaussianOverlap{
		stdDev: TimestampInNano(0.05), // x seconds
		cutoff: 4,                     // x standard deviations
	}

	time1 := time.Now()
	// build cooccurrence map
	sessionTimestamps := makeTimestamps(&overlapFunctions, records)
	cooc, earliestTimestamp := makeCoOccurrence(sessionTimestamps)
	cooc.SaveData(coOccurrencePath)
	fmt.Printf("Cooccurrence took %v\n", time.Since(time1))

	regionLeeway := uint64(3)
	regions := ConstructTestSessionRegions(earliestTimestamp, regionLeeway)

	customScoreFunc := func(X uint64, Y float64, Z float64) (float64, error) {
		path := fmt.Sprintf("data/ghc/ts2_cooc_%f.pb", Z)
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
				stdDev: TimestampInNano(Z), // x seconds
				cutoff: 4,                  // x standard deviations
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

		// opticsOpts := fmt.Sprintf("min_samples=%d,max_eps=%f", X, Y)
		// clusterMethod := NewOptics(opticsOpts)
		hdbscanOpts := fmt.Sprintf("min_cluster_size=%d,cluster_selection_epsilon=%f", X, Y)
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
		fmt.Printf("%v:%v,%v ", clusterMethod.Name(), clusterMethod.Args(), Z)
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
		best := hof.Genome.(*Coord3D)
		fmt.Printf("Best fitness at generation %4d: %10.5f at (%d, %9.5f, %.5f)\n",
			ga.Generations, fit, best.X, best.Y, best.Z)
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
			return &Coord3D{
				X:         7,
				Y:         0.2,
				Z:         0.03,
				scoreFunc: customScoreFunc,
			}
		}
		return &Coord3D{
			X:         uint64(rng.Intn(8) + 2), // range: 2-10
			Y:         rng.Float64(),
			Z:         rng.Float64(),
			scoreFunc: customScoreFunc,
		}
	})
	if err != nil {
		panic(err)
	}

	// output the best encountered solution.
	best := ga.HallOfFame[0].Genome.(*Coord3D)
	bestScore := ga.HallOfFame[0].Fitness
	fmt.Printf("Found a minimum at (%d, %.5f, %.5f) with score %v.\n", best.X, best.Y, best.Z, bestScore)
}
