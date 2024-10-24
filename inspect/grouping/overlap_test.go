package grouping

import (
	"fmt"
	"math"
	"testing"
)

func TestFixedMarginOverlap(t *testing.T) {
	tests := []struct {
		name        string
		fixedMargin uint64
		times1      []uint64
		times2      []uint64
		expected    uint64
	}{
		{
			name:        "Non-overlapping times",
			fixedMargin: 10,
			times1:      []uint64{100, 200, 300},
			times2:      []uint64{150, 250, 350},
			expected:    0,
		},
		{
			name:        "Overlapping (intervals overlap only one other)",
			fixedMargin: 10,
			times1:      []uint64{100, 200},
			times2:      []uint64{110, 210},
			expected:    20,
		},
		{
			name:        "Overlapping (intervals overlap multiple others)",
			fixedMargin: 30,
			times1:      []uint64{100, 200, 300},
			times2:      []uint64{150, 250, 350},
			expected:    50,
		},
		{
			name:        "Overlapping (self overlap partially) [check for double counting]",
			fixedMargin: 60,
			times1:      []uint64{100, 200, 300},
			times2:      []uint64{150, 250, 350},
			expected:    270,
		},
		{
			name:        "Overlapping (self overlap full intervals) [check for double counting]",
			fixedMargin: 100,
			times1:      []uint64{100, 200, 300},
			times2:      []uint64{150, 250, 350},
			expected:    350,
		},
		{
			name:        "Overlapping (same times)",
			fixedMargin: 100,
			times1:      []uint64{100, 200, 300},
			times2:      []uint64{100, 200, 300},
			expected:    400,
		},
		{
			name:        "No margin",
			fixedMargin: 0,
			times1:      []uint64{100, 200, 300},
			times2:      []uint64{100, 200, 300},
			expected:    0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fmo := FixedMarginOverlap{Margin: tc.fixedMargin}
			actual := fmo.CalculateOverlap(tc.times1, tc.times2)
			if actual != tc.expected {
				t.Errorf("Test %s failed: expected %d but got %d", tc.name, tc.expected, actual)
			}
		})
	}
}

func TestGaussianOverlap(t *testing.T) {
	tests := []struct {
		name     string
		stdDev   uint64
		times1   []uint64
		times2   []uint64
		expected float64 // expected overlap in seconds
	}{
		{
			name:     "Non-overlapping times",
			stdDev:   1,
			times1:   []uint64{100},
			times2:   []uint64{150},
			expected: 0.0,
		},
		{
			name:     "Single overlap times",
			stdDev:   30,
			times1:   []uint64{100},
			times2:   []uint64{150},
			expected: 0.404657,
		},
		{
			name:     "Multiple overlapping times",
			stdDev:   10,
			times1:   []uint64{100, 200, 300},
			times2:   []uint64{150, 250, 350},
			expected: 0.06177994083,
		},
		{
			name:     "Self-overlap",
			stdDev:   30,
			times1:   []uint64{200},
			times2:   []uint64{150, 250},
			expected: 0.8091868388872397,
		},
		{
			name:     "Complex overlap 1",
			stdDev:   30,
			times1:   []uint64{100, 200, 300},
			times2:   []uint64{150, 250, 350},
			expected: 2.0600350617217575,
		},
		{
			name:     "Complex overlap 2",
			stdDev:   30,
			times1:   []uint64{100, 180, 225, 270, 315},
			times2:   []uint64{150, 220, 250, 350, 400},
			expected: 6.209881266133925,
		},
		{
			name:     "Complex overlap 3",
			stdDev:   50,
			times1:   []uint64{100, 200, 300},
			times2:   []uint64{150, 250, 350},
			expected: 3.498067843171574,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cutoff := 4 * tc.stdDev // 4 stdev contains 99.99% of the distribution
			gausso := GaussianOverlap{StdDev: tc.stdDev, Cutoff: cutoff}
			actual := TimestampInSeconds(gausso.CalculateOverlap(tc.times1, tc.times2))
			percent := (1 - actual/tc.expected) * 100.0
			if tc.expected != 0 {
				fmt.Printf("Result(%.6f) is %.6f%% away from expected(%.6f)\n", actual, percent, tc.expected)
			}
			// check if actual is within 1% of expected
			if math.Abs(percent) > 1 {
				t.Errorf("Test %s failed: expected %.6f but got %.6f", tc.name, tc.expected, actual)
			}

		})
	}
}
