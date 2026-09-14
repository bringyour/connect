package emoji

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/rivo/uniseg"
)

// Every pool entry is one Emoji_Presentation code point that ValidateTag
// accepts as exactly one emoji, and no entry repeats.
func TestSuggestPool(t *testing.T) {
	if len(suggestPool) < 140 {
		t.Fatalf("pool has %d emoji, want at least 140", len(suggestPool))
	}
	seen := map[rune]bool{}
	for _, r := range suggestPool {
		if seen[r] {
			t.Errorf("duplicate pool entry %U", r)
		}
		seen[r] = true
		if !IsEmojiPresentation(r) {
			t.Errorf("%U (%s) is not Emoji_Presentation", r, string(r))
		}
		normalized, count, err := ValidateTag(string(r))
		if err != nil || count != 1 || normalized != string(r) {
			t.Errorf("ValidateTag(%U) = %q, %d, %v", r, normalized, count, err)
		}
	}
	if SuggestPoolSize() != len(suggestPool) {
		t.Fatal("SuggestPoolSize")
	}
}

func TestSuggest(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	for i := 0; i < 500; i++ {
		count := i%6 - 1 // -1, 0, 1, 2, 3, 4
		tag := Suggest(count, r)
		normalized, n, err := ValidateTag(tag)
		if err != nil || normalized != tag {
			t.Fatalf("Suggest(%d) = %q: %v", count, tag, err)
		}
		switch {
		case count <= 0:
			if n < 1 || SuggestMaxEmoji < n {
				t.Fatalf("Suggest(%d) = %q has %d emoji", count, tag, n)
			}
		case SuggestMaxEmoji < count:
			if n != SuggestMaxEmoji {
				t.Fatalf("Suggest(%d) = %q has %d emoji, want %d", count, tag, n, SuggestMaxEmoji)
			}
		default:
			if n != count {
				t.Fatalf("Suggest(%d) = %q has %d emoji", count, tag, n)
			}
		}
		// distinct emoji
		clusters := map[string]bool{}
		g := uniseg.NewGraphemes(tag)
		for g.Next() {
			if clusters[g.Str()] {
				t.Fatalf("Suggest(%d) = %q repeats %q", count, tag, g.Str())
			}
			clusters[g.Str()] = true
		}
	}
	// a random length covers the whole 1..3 range
	lengths := map[int]bool{}
	for i := 0; i < 200; i++ {
		lengths[Count(Suggest(0, r))] = true
	}
	if len(lengths) != SuggestMaxEmoji {
		t.Fatalf("random lengths = %v", lengths)
	}
	// seeded sources are reproducible; nil uses the shared source
	if Suggest(3, rand.New(rand.NewSource(1))) != Suggest(3, rand.New(rand.NewSource(1))) {
		t.Fatal("seeded Suggest is not reproducible")
	}
	if tag := Suggest(2, nil); Count(tag) != 2 || strings.TrimSpace(tag) != tag {
		t.Fatalf("Suggest(2, nil) = %q", tag)
	}
}
