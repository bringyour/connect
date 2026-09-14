package emoji

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateTag(t *testing.T) {
	cases := []struct {
		name       string
		in         string
		normalized string
		count      int
		err        error
	}{
		{"one emoji", "🐬", "🐬", 1, nil},
		{"six emoji", "🐬🔥🚀🌊🎯🏆", "🐬🔥🚀🌊🎯🏆", 6, nil},
		{"seven emoji", "🐬🔥🚀🌊🎯🏆🐸", "", 0, ErrTooMany},
		{"empty", "", "", 0, ErrEmpty},
		{"whitespace only", " \t\n", "", 0, ErrEmpty},
		{"surrounding whitespace trimmed", "  🐬🔥 ", "🐬🔥", 2, nil},
		{"family zwj sequence", "👨‍👩‍👧‍👦", "👨‍👩‍👧‍👦", 1, nil},
		{"profession zwj with skin tone", "👩🏽‍🚀", "👩🏽‍🚀", 1, nil},
		{"rainbow flag zwj", "🏳️‍🌈", "🏳️‍🌈", 1, nil},
		{"heart on fire zwj", "❤️‍🔥", "❤️‍🔥", 1, nil},
		{"country flag", "🇺🇸", "🇺🇸", 1, nil},
		{"two flags", "🇺🇸🇯🇵", "🇺🇸🇯🇵", 2, nil},
		{"subdivision flag", "🏴󠁧󠁢󠁳󠁣󠁴󠁿", "🏴󠁧󠁢󠁳󠁣󠁴󠁿", 1, nil},
		{"keycap with vs16", "1️⃣", "1️⃣", 1, nil},
		{"keycap without vs16 normalized", "1⃣", "1️⃣", 1, nil},
		{"keycap hash", "#️⃣", "#️⃣", 1, nil},
		{"skin tone thumbs up", "👍🏿", "👍🏿", 1, nil},
		{"text default pictograph gets vs16", "☺", "☺️", 1, nil},
		{"text default with vs16 kept", "☺️", "☺️", 1, nil},
		{"copyright promoted", "©", "©️", 1, nil},
		{"text presentation selector rejected", "☺︎", "", 0, ErrNotEmoji},
		{"letter", "a", "", 0, ErrNotEmoji},
		{"letters with emoji", "gg🐬", "", 0, ErrNotEmoji},
		{"digit alone", "1", "", 0, ErrNotEmoji},
		{"digits", "123", "", 0, ErrNotEmoji},
		{"punctuation", "!", "", 0, ErrNotEmoji},
		{"emoji then space then emoji", "🐬 🔥", "", 0, ErrNotEmoji},
		{"lone regional indicator", "🇺", "", 0, ErrNotEmoji},
		{"three regional indicators", "🇺🇸🇯", "", 0, ErrNotEmoji},
		{"lone zwj", "‍", "", 0, ErrNotEmoji},
		{"lone vs16", "️", "", 0, ErrNotEmoji},
		{"lone skin tone modifier", "🏽", "", 0, ErrNotEmoji},
		{"trailing zwj", "👍‍", "", 0, ErrNotEmoji},
		{"zwj to letter", "👍‍a", "", 0, ErrNotEmoji},
		{"combining mark on letter", "é", "", 0, ErrNotEmoji},
		{"combining mark on emoji", "🐬́", "", 0, ErrNotEmoji},
		{"unassigned reserved block", string(rune(0x1FC00)), "", 0, ErrNotEmoji},
		{"cjk", "漢", "", 0, ErrNotEmoji},
		{"invalid utf8", "\xff\xfe", "", 0, ErrNotEmoji},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			normalized, count, err := ValidateTag(c.in)
			if !errors.Is(err, c.err) {
				t.Fatalf("err = %v, want %v", err, c.err)
			}
			if normalized != c.normalized {
				t.Fatalf("normalized = %q (%U), want %q (%U)", normalized, []rune(normalized), c.normalized, []rune(c.normalized))
			}
			if count != c.count {
				t.Fatalf("count = %d, want %d", count, c.count)
			}
		})
	}
}

func TestIsTagAndCount(t *testing.T) {
	if !IsTag("🐬🔥") || IsTag("ab") || IsTag("") {
		t.Fatal("IsTag")
	}
	if Count("🐬🔥🚀") != 3 || Count("x") != 0 {
		t.Fatal("Count")
	}
	// exactly six is the cap
	six := strings.Repeat("🔥", 6)
	if Count(six) != 6 || IsTag(six+"🔥") {
		t.Fatal("cap")
	}
}

func TestTables(t *testing.T) {
	for name, ranges := range map[string][]runeRange{"extendedPictographic": extendedPictographic, "emojiPresentation": emojiPresentation} {
		for i, r := range ranges {
			if r.hi < r.lo {
				t.Fatalf("%s[%d] hi < lo", name, i)
			}
			if 0 < i && r.lo <= ranges[i-1].hi {
				t.Fatalf("%s[%d] not sorted/disjoint", name, i)
			}
		}
	}
	if !IsExtendedPictographic('🐬') || IsExtendedPictographic('a') {
		t.Fatal("IsExtendedPictographic")
	}
	if !IsEmojiPresentation('🐬') || IsEmojiPresentation('©') {
		t.Fatal("IsEmojiPresentation")
	}
}
