package connect

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	// "math"
	mathrand "math/rand"
)

func TestMonitor(t *testing.T) {
	timeout := 1 * time.Second

	monitor := NewMonitor()

	for i := 0; i < 32; i += 1 {
		update := monitor.NotifyChannel()
		go monitor.NotifyAll()
		select {
		case <-update:
		case <-time.After(timeout):
			t.Fail()
		}
	}

}

func TestCallbackList(t *testing.T) {
	callbacks := NewCallbackList[func(int)]()

	n := 10
	m := 100

	testingCallbacks := []*testingCallback{}
	for i := 0; i < n; i += 1 {
		testingCallbacks = append(testingCallbacks, &testingCallback{})
	}

	testingCallbackIds := []int{}

	AssertEqual(t, 0, len(callbacks.Get()))
	for i := 0; i < n; i += 1 {
		callbackId := callbacks.Add(testingCallbacks[i].Callback)
		testingCallbackIds = append(testingCallbackIds, callbackId)
		AssertEqual(t, i+1, len(callbacks.Get()))
	}
	AssertEqual(t, n, len(callbacks.Get()))
	// note callbacks can be added multiple times

	mathrand.Shuffle(len(testingCallbackIds), func(i int, j int) {
		testingCallbackIds[i], testingCallbackIds[j] = testingCallbackIds[j], testingCallbackIds[i]
	})

	for i := 0; i < n; i += 1 {
		callbacks.Remove(testingCallbackIds[i])
		AssertEqual(t, n-1-i, len(callbacks.Get()))
	}
	AssertEqual(t, 0, len(callbacks.Get()))

	for i := 0; i < n; i += 1 {
		callbacks.Add(testingCallbacks[i].Callback)
		AssertEqual(t, i+1, len(callbacks.Get()))
	}

	for i := 0; i < m; i += 1 {
		for _, callback := range callbacks.Get() {
			callback(i)
		}
	}

	for i := 0; i < n; i += 1 {
		values := testingCallbacks[i].Values()
		for j := 0; j < m; j += 1 {
			AssertEqual(t, j, values[j])
		}
	}

}

type testingCallback struct {
	stateLock sync.Mutex
	values    []int
}

func (self *testingCallback) Callback(value int) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.values = append(self.values, value)
}

func (self *testingCallback) Values() []int {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	valuesCopy := slices.Clone(self.values)
	return valuesCopy
}

func TestIdleCondition(t *testing.T) {
	idleCondition := NewIdleCondition()

	a := idleCondition.Checkpoint()
	success := idleCondition.UpdateOpen()
	AssertEqual(t, true, success)
	success = idleCondition.Close(a)
	AssertEqual(t, false, success)
	success = idleCondition.UpdateOpen()
	AssertEqual(t, true, success)
	success = idleCondition.Close(a)
	AssertEqual(t, false, success)
	idleCondition.UpdateClose()
	idleCondition.UpdateClose()
	b := idleCondition.Checkpoint()
	go func() {
		success := idleCondition.Close(b)
		AssertEqual(t, true, success)
	}()
	success = idleCondition.WaitForClose()
	AssertEqual(t, true, success)

}

func TestEvent(t *testing.T) {
	event := NewEvent()
	go func() {
		event.Set()
	}()
	success := event.WaitForSet(30 * time.Second)
	AssertEqual(t, true, success)
}

func TestResetOrCreateTimerSteadyStateDoesNotAllocate(t *testing.T) {
	var timer *time.Timer
	resetOrCreateTimer(&timer, time.Hour)
	defer timer.Stop()

	allocs := testing.AllocsPerRun(1000, func() {
		resetOrCreateTimer(&timer, time.Hour)
		timer.Stop()
	})

	AssertEqual(t, 0.0, allocs)
}

// TestPacedReconnectWaitsForMinimum verifies the predictable reconnect variant
// never turns its configured rate floor into full jitter.
func TestPacedReconnectWaitsForMinimum(t *testing.T) {
	minTimeout := 30 * time.Millisecond
	startTime := time.Now()
	<-NewPacedReconnect(minTimeout).After()
	elapsed := time.Since(startTime)
	if elapsed < minTimeout {
		t.Fatalf("paced reconnect waited %s, want at least %s", elapsed, minTimeout)
	}
}

// TestPacedReconnectCompletesAfterAttemptConsumesMinimum verifies a slow
// attempt does not incur an additional delay.
func TestPacedReconnectCompletesAfterAttemptConsumesMinimum(t *testing.T) {
	reconnect := NewPacedReconnect(10 * time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	select {
	case <-reconnect.After():
	default:
		t.Fatal("paced reconnect did not complete after its minimum elapsed")
	}
}

func TestMinTime(t *testing.T) {
	a := time.Now()
	n := 10
	bs := make([]time.Time, n)
	bs[0] = a
	for i := 1; i < n; i += 1 {
		bs[i] = bs[i-1].Add(time.Second)
	}
	mathrand.Shuffle(len(bs), func(i int, j int) {
		bs[i], bs[j] = bs[j], bs[i]
	})

	foundA := MinTime(bs[0], bs[1:]...)
	AssertEqual(t, a, foundA)
}

// Keep size/samplesPerValue at four: for a uniform ranking, the standard error
// of each sampled mean position is approximately sqrt(size/(12*samplesPerValue)).
// These values therefore retain the prior test's statistical resolution and
// exact rank tolerances while avoiding its sixteen-fold excess O(k*n^3) work.
const (
	weightedShuffleTestSize                  = 128
	weightedShuffleTestSamplesPerValue       = 32
	weightedShuffleTestSeed            int64 = 0x5753485546464c45
)

func TestWeightedShuffle(t *testing.T) {
	// weighted shuffle many times and look at the average position
	// the average position order should trend with the weight order

	if testing.Short() {
		t.Skip("skipping testing in short mode")
	}

	k := weightedShuffleTestSamplesPerValue
	n := weightedShuffleTestSize
	random := mathrand.New(mathrand.NewSource(weightedShuffleTestSeed))

	netIndexes1 := map[int]int64{}
	netIndexes2 := map[int]int64{}
	netIndexes3 := map[int]int64{}
	values := make([]int, n)
	weights := make(map[int]float32, n)
	for value := range n {
		weights[value] = float32(n - value)
	}

	for i := 0; i < n*k; i += 1 {
		if i%100 == 0 {
			fmt.Printf("[w]%d/%d\n", i+1, n*k)
		}

		for value := range n {
			values[value] = value
		}

		weightedShuffleWithEntropy(values, weights, float32(0), random)
		for index, value := range values {
			netIndexes1[value] += int64(index)
		}

		weightedShuffleFuncWithEntropy(values, func(i int) float32 {
			return weights[i]
		}, float32(0), random)
		for index, value := range values {
			netIndexes2[value] += int64(index)
		}

		weightedSelectFuncWithEntropy(values, len(values), func(i int) float32 {
			return weights[i]
		}, float32(0), random)
		for index, value := range values {
			netIndexes3[value] += int64(index)
		}
	}

	test := func(netIndexes map[int]int64) {
		orderedValues := []int{}
		for i := 0; i < n; i += 1 {
			orderedValues = append(orderedValues, i)
		}
		slices.SortFunc(orderedValues, func(a int, b int) int {
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
			e := i - orderedValues[i]
			if -errorThreshold <= e && e <= errorThreshold {
				e = 0
			}
			AssertEqual(t, 0, e)
		}
	}

	test(netIndexes1)
	test(netIndexes2)
	test(netIndexes3)
}

func TestWeightedShuffleWithEntropy(t *testing.T) {
	// as entropy approaches 1, the weighted shuffle should become uniform

	if testing.Short() {
		t.Skip("skipping testing in short mode")
	}

	k := weightedShuffleTestSamplesPerValue
	n := weightedShuffleTestSize
	weights := make(map[int]float32, n)
	for value := range n {
		weights[value] = float32(n - value)
	}

	orderedEntropies := []float32{
		0.0,
		0.5,
		1.0,
	}

	for entropyIndex, entropy := range orderedEntropies {
		random := mathrand.New(mathrand.NewSource(weightedShuffleTestSeed + int64(entropyIndex) + 1))
		netIndexes1 := map[int]int64{}
		netIndexes2 := map[int]int64{}
		netIndexes3 := map[int]int64{}
		values := make([]int, n)

		for i := 0; i < n*k; i += 1 {
			if i%100 == 0 {
				fmt.Printf("[we]%d/%d\n", i+1, n*k)
			}

			for value := range n {
				values[value] = value
			}

			weightedShuffleWithEntropy(values, weights, entropy, random)
			for index, value := range values {
				netIndexes1[value] += int64(index)
			}

			weightedShuffleFuncWithEntropy(values, func(i int) float32 {
				return weights[i]
			}, entropy, random)
			for index, value := range values {
				netIndexes2[value] += int64(index)
			}

			weightedSelectFuncWithEntropy(values, len(values), func(i int) float32 {
				return weights[i]
			}, entropy, random)
			for index, value := range values {
				netIndexes3[value] += int64(index)
			}
		}

		testError := func(testEntropy float32, expected bool, netIndexes map[int]int64) {
			// n * k * ((1-e)*n + e*n/k)
			// == n^2 * ((1-e)*k+e)
			errorThreshold := int64(float32(n) * float32(n) * ((1-testEntropy)*float32(k) + testEntropy))
			failed := false
			for i := 1; i < n; i += 1 {
				a := i / 2
				b := n - (i+1)/2
				e := netIndexes[a] - netIndexes[b]
				if -errorThreshold <= e && e <= errorThreshold {
					e = 0
				}
				if expected {
					// all must pass
					AssertEqual(t, int64(0), e)
				} else if int64(0) != e {
					failed = true
				}
			}
			if !expected {
				// at least one of the comparisons must have failed
				// not all must fail
				AssertEqual(t, true, failed)
			}
		}

		fmt.Printf("[entropy]%d\n", entropyIndex)
		testError(entropy, true, netIndexes1)
		testError(entropy, true, netIndexes2)
		testError(entropy, true, netIndexes3)
		// the test should fail at the next entropy index (tigher error bound)
		if entropyIndex+1 < len(orderedEntropies) {
			testError(orderedEntropies[entropyIndex+1], false, netIndexes1)
			testError(orderedEntropies[entropyIndex+1], false, netIndexes2)
			testError(orderedEntropies[entropyIndex+1], false, netIndexes3)
		}
	}
}

// TestMemoryShedders: the registry runs registered shedders on ShedMemory, and an
// unregistered shedder no longer runs.
func TestMemoryShedders(t *testing.T) {
	var mu sync.Mutex
	count := 0
	unregister := AddMemoryShedder(func() {
		mu.Lock()
		defer mu.Unlock()
		count += 1
	})

	ShedMemory()
	ShedMemory()
	mu.Lock()
	got := count
	mu.Unlock()
	if got != 2 {
		t.Fatalf("shedder ran %d times, want 2", got)
	}

	unregister()
	ShedMemory()
	mu.Lock()
	got = count
	mu.Unlock()
	if got != 2 {
		t.Fatalf("shedder ran %d times after unregister, want still 2", got)
	}
}
