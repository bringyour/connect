package connect

import (
    "testing"
    "slices"
    // "math"
    // "fmt"

    "github.com/go-playground/assert/v2"
)


func TestWeightedShuffle(t *testing.T) {
	// weighted shuffle many times and look at the average position
	// the average position order should trend with the weight order
	
	k := 64
	n := 256

	netIndexes := map[int]int64{}

	for i := 0; i < n * k; i += 1 {
		values := []int{}
		weights := map[int]float32{}
		for j := 0; j < n; j += 1 {
			values = append(values, j)
			weights[j] = float32(n - j)
		}

		WeightedShuffle(values, weights)
		for index, value := range values {
			netIndexes[value] += int64(index)
		}
	}

	values := []int{}
	for i := 0; i < n; i += 1 {
		values = append(values, i)
	}
	slices.SortFunc(values, func(a int, b int)(int) {
		if netIndexes[a] < netIndexes[b] {
			return -1
		} else if netIndexes[b] < netIndexes[a] {
			return 1
		} else {
			return 0
		}
	})

	errorThreshold := 2 * n / k
	for i := 0; i < n; i += 1 {
		e := i - values[i]
		if -errorThreshold <= e && e <= errorThreshold {
			e = 0
		}
		assert.Equal(t, 0, e)
	}
}

