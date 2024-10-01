package main

import (
	"fmt"
	"log"
	"math"
	"math/rand"

	"bringyour.com/inspect/data"
	"github.com/MaxHalford/eaopt"
)

// A Coord2D is a coordinate in two dimensions.
type Coord2D struct {
	X         uint64  // min_samples
	Y         float64 // max_eps
	scoreFunc func(X uint64, Y float64) (float64, error)
}

// Evaluate evalutes a Bohachevsky function at the current coordinates.
func (c *Coord2D) Evaluate() (float64, error) {
	return c.scoreFunc(c.X, c.Y)
}

// Mutate replaces one of the current coordinates with a random value in [-100, -100].
func (c *Coord2D) Mutate(rng *rand.Rand) {
	if rng.Intn(2) == 0 {
		// change X
		change := uint64(rng.Intn(2) + 1) // 1 or 2
		if rng.Intn(2) == 0 {
			c.X += change
		} else {
			c.X -= change
		}
		c.X = max(2, c.X) // X >= 2
	} else {
		change := rng.Float64() / 5.0 // range 0-0.2
		if rng.Intn(2) == 0 {
			c.Y += change
		} else {
			c.Y -= change
		}
		c.Y = math.Min(1, math.Max(0.1, c.Y)) // range 0.1-1
	}
}

// Crossover does nothing.  It is defined only so *Coord2D implements the eaopt.Genome interface.
func (c *Coord2D) Crossover(other eaopt.Genome, rng *rand.Rand) {}

// Clone returns a copy of a *Coord2D.
func (c *Coord2D) Clone() eaopt.Genome {
	return &Coord2D{X: c.X, Y: c.Y, scoreFunc: c.scoreFunc}
}

func GeneticHillClimbing(savePath string, coOccurrencePath string) {
	records, err := data.LoadTransportsFromFiles(savePath)
	if err != nil {
		log.Fatalf("Error loading transports: %v", err)
	}

	// build cooccurrence map
	sessionTimestamps := makeTimestamps(records)
	cooc, earliestTimestamp := makeCoOccurrence(sessionTimestamps)
	cooc.SaveData(coOccurrencePath)

	regionLeeway := uint64(3)
	regions := ConstructTestSession1Regions(earliestTimestamp, regionLeeway)

	customScoreFunc := func(X uint64, Y float64) (float64, error) {
		// opticsOpts := fmt.Sprintf("min_samples=%d,max_eps=%f", X, Y)
		// clusterMethod := NewOptics(opticsOpts)
		hdbscanOpts := fmt.Sprintf("min_cluster_size=%d,cluster_selection_epsilon=%f", X, Y)
		clusterMethod := NewHDBSCAN(hdbscanOpts)

		clusterOps := &ClusterOpts{
			ClusterMethod:    clusterMethod,
			CoOccurrencePath: coOccurrencePath,
		}
		clusters, err := cluster(clusterOps, false)
		if err != nil {
			return 0, fmt.Errorf("error clustering: %v", err)
		}
		fmt.Printf("%v:%v ", clusterMethod.Name(), clusterMethod.Args())
		score := Evaluate(*sessionTimestamps, *regions, clusters)
		return -1 * score, nil
	}

	// Hill climbing is implemented as a GA using the ModMutationOnly model
	// with the Strict option.
	cfg := eaopt.NewDefaultGAConfig()
	cfg.Model = eaopt.ModMutationOnly{Strict: true}
	cfg.PopSize = 2
	cfg.ParallelEval = true
	cfg.NGenerations = 20

	// Add a custom callback function to track progress.
	minFit := math.MaxFloat64
	cfg.Callback = func(ga *eaopt.GA) {
		hof := ga.HallOfFame[0]
		fit := hof.Fitness
		if fit == minFit {
			// Output only when we make an improvement.
			return
		}
		best := hof.Genome.(*Coord2D)
		fmt.Printf("Best fitness at generation %4d: %10.5f at (%d, %9.5f)\n",
			ga.Generations, fit, best.X, best.Y)
		minFit = fit
	}

	// Run the hill-climbing algorithm.
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
			return &Coord2D{
				X:         4,
				Y:         0.2,
				scoreFunc: customScoreFunc,
			}
		}
		return &Coord2D{
			X:         uint64(rng.Intn(8) + 2), // 2-10
			Y:         rng.Float64(),
			scoreFunc: customScoreFunc,
		}
	})
	if err != nil {
		panic(err)
	}

	// Output the best encountered solution.
	best := ga.HallOfFame[0].Genome.(*Coord2D)
	bestScore := ga.HallOfFame[0].Fitness
	fmt.Printf("Found a minimum at (%d, %.5f) with score %v.\n", best.X, best.Y, bestScore)
}

func GeneticOES(savePath string, coOccurrencePath string) {
	records, err := data.LoadTransportsFromFiles(savePath)
	if err != nil {
		log.Fatalf("Error loading transports: %v", err)
	}

	// build cooccurrence map
	sessionTimestamps := makeTimestamps(records)
	cooc, earliestTimestamp := makeCoOccurrence(sessionTimestamps)
	cooc.SaveData(coOccurrencePath)

	regionLeeway := uint64(3)
	regions := ConstructTestSession1Regions(earliestTimestamp, regionLeeway)

	customScoreFunc := func(x []float64) float64 {
		X := int64(10 * x[0])
		Y := x[1]
		if X < 2 || Y > 1 || Y < 0.1 {
			fmt.Println("Invalid values ", X, Y)
			return 0.0 + math.Abs(0.5-Y) + math.Abs(2-float64(X))
		}
		fmt.Println(X, Y)
		opticsOpts := fmt.Sprintf("min_samples=%d,max_eps=%f", X, Y)
		clusterMethod := NewOptics(opticsOpts)
		clusterOps := &ClusterOpts{
			ClusterMethod:    clusterMethod,
			CoOccurrencePath: coOccurrencePath,
		}
		clusters, err := cluster(clusterOps, false)
		if err != nil {
			return 1000
		}
		score := Evaluate(*sessionTimestamps, *regions, clusters)
		return -1.0 * score
	}

	randomness := rand.New(rand.NewSource(42))

	oes, err := eaopt.NewOES(uint(4), uint(20), float64(1), float64(0.2), true, randomness)
	if err != nil {
		fmt.Println(err)
		return
	}

	// first float is mult by 10
	res, y, err := oes.Minimize(customScoreFunc, []float64{0.4, 0.9})
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("Found minimum of %v at %.5f\n", res, y)
}
