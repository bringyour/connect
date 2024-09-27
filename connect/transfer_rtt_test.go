package connect

import (
	"testing"
	"time"

	"github.com/go-playground/assert/v2"
)

func TestRttWindow(t *testing.T) {
	rttWindow := NewRttWindow(4, 1*time.Second, 1.0, 0, time.Second)

	assert.Equal(t, rttWindow.ScaledRtt(), time.Duration(0))

	start := time.Now()

	tag1 := rttWindow.openTag(start)
	tag2 := rttWindow.openTag(start.Add(50 * time.Millisecond))
	tag3 := rttWindow.openTag(start.Add(100 * time.Millisecond))
	tag4 := rttWindow.openTag(start.Add(150 * time.Millisecond))

	assert.Equal(t, rttWindow.scaledRtt(start.Add(150*time.Millisecond)), time.Duration(0))

	rttWindow.closeTag(tag2, start.Add(300*time.Millisecond)) // 250

	assert.Equal(t, rttWindow.ScaledRtt(), 250*time.Millisecond)

	rttWindow.closeTag(tag4, start.Add(300*time.Millisecond)) // 150
	rttWindow.closeTag(tag3, start.Add(500*time.Millisecond)) // 400
	rttWindow.closeTag(tag1, start.Add(800*time.Millisecond)) // 800

	assert.Equal(t, rttWindow.scaledRtt(start.Add(800*time.Millisecond)), (250+150+400+800)/4*time.Millisecond)

	start2 := start.Add(2 * time.Second)
	tag21 := rttWindow.openTag(start2)
	tag22 := rttWindow.openTag(start2)
	tag23 := rttWindow.openTag(start2)
	tag24 := rttWindow.openTag(start2)
	tag25 := rttWindow.openTag(start2)

	// clears the window
	rttWindow.closeTag(tag21, start2.Add(500*time.Millisecond))

	assert.Equal(t, rttWindow.scaledRtt(start2.Add(500*time.Millisecond)), 500*time.Millisecond)

	rttWindow.closeTag(tag22, start2.Add(500*time.Millisecond))

	assert.Equal(t, rttWindow.scaledRtt(start2.Add(500*time.Millisecond)), 500*time.Millisecond)

	rttWindow.closeTag(tag23, start2.Add(500*time.Millisecond))
	rttWindow.closeTag(tag24, start2.Add(500*time.Millisecond))

	// cycle window
	rttWindow.closeTag(tag25, start2.Add(100*time.Millisecond))

	assert.Equal(t, rttWindow.scaledRtt(start2.Add(100*time.Millisecond)), (500+500+500+100)/4*time.Millisecond)
}
