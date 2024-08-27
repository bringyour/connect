package connect

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
	// "math"
	mathrand "math/rand"

	"github.com/go-playground/assert/v2"
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

	assert.Equal(t, 0, len(callbacks.Get()))
	for i := 0; i < n; i += 1 {
		callbackId := callbacks.Add(testingCallbacks[i].Callback)
		testingCallbackIds = append(testingCallbackIds, callbackId)
		assert.Equal(t, i+1, len(callbacks.Get()))
	}
	assert.Equal(t, n, len(callbacks.Get()))
	// note callbacks can be added multiple times

	mathrand.Shuffle(len(testingCallbackIds), func(i int, j int) {
		testingCallbackIds[i], testingCallbackIds[j] = testingCallbackIds[j], testingCallbackIds[i]
	})

	for i := 0; i < n; i += 1 {
		callbacks.Remove(testingCallbackIds[i])
		assert.Equal(t, n-1-i, len(callbacks.Get()))
	}
	assert.Equal(t, 0, len(callbacks.Get()))

	for i := 0; i < n; i += 1 {
		callbacks.Add(testingCallbacks[i].Callback)
		assert.Equal(t, i+1, len(callbacks.Get()))
	}

	for i := 0; i < m; i += 1 {
		for _, callback := range callbacks.Get() {
			callback(i)
		}
	}

	for i := 0; i < n; i += 1 {
		values := testingCallbacks[i].Values()
		for j := 0; j < m; j += 1 {
			assert.Equal(t, j, values[j])
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
	assert.Equal(t, true, success)
	success = idleCondition.Close(a)
	assert.Equal(t, false, success)
	success = idleCondition.UpdateOpen()
	assert.Equal(t, true, success)
	success = idleCondition.Close(a)
	assert.Equal(t, false, success)
	idleCondition.UpdateClose()
	idleCondition.UpdateClose()
	b := idleCondition.Checkpoint()
	go func() {
		success := idleCondition.Close(b)
		assert.Equal(t, true, success)
	}()
	success = idleCondition.WaitForClose()
	assert.Equal(t, true, success)

}

func TestEvent(t *testing.T) {
	event := NewEvent()
	go func() {
		event.Set()
	}()
	success := event.WaitForSet(30 * time.Second)
	assert.Equal(t, true, success)
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
	assert.Equal(t, a, foundA)
}

func TestWeightedShuffle(t *testing.T) {
	// weighted shuffle many times and look at the average position
	// the average position order should trend with the weight order

	k := 64
	n := 512

	netIndexes := map[int]int64{}

	for i := 0; i < n*k; i += 1 {
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
		assert.Equal(t, 0, e)
	}
}

func TestWeightedShuffleWithEntropy(t *testing.T) {
	// as entropy approaches 1, the weighted shuffle should become uniform

	k := 64
	n := 256

	orderedEntropies := []float32{
		0.0,
		0.5,
		1.0,
	}

	for entropyIndex, entropy := range orderedEntropies {
		netIndexes := map[int]int64{}

		for i := 0; i < n*k; i += 1 {
			values := []int{}
			weights := map[int]float32{}
			for j := 0; j < n; j += 1 {
				values = append(values, j)
				weights[j] = float32(n - j)
			}

			WeightedShuffleWithEntropy(values, weights, entropy)
			for index, value := range values {
				netIndexes[value] += int64(index)
			}
		}

		testError := func(testEntropy float32, expected bool) {
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
					assert.Equal(t, int64(0), e)
				} else if int64(0) != e {
					failed = true
				}
			}
			if !expected {
				// at least one of the comparisons must have failed
				// not all must fail
				assert.Equal(t, true, failed)
			}
		}

		fmt.Printf("[entropy]%d\n", entropyIndex)
		testError(entropy, true)
		// the test should fail at the next entropy index (tigher error bound)
		if entropyIndex+1 < len(orderedEntropies) {
			testError(orderedEntropies[entropyIndex+1], false)
		}
	}
}
