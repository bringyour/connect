// Package emoji validates the emoji-only tag a network can attach to its name
// (the points leaderboard identity): one to six emoji, nothing else.
//
// The same function runs on the server (the source of truth for what is
// stored) and in the sdk (so an editor can reject a character before the
// request goes out), which is why it lives in connect and not in either.
//
// A "tag" is a sequence of grapheme clusters, each of which must be a single
// emoji as a keyboard produces it:
//
//   - a pictograph (Extended_Pictographic), optionally with the emoji
//     presentation selector U+FE0F and a skin tone modifier;
//   - a ZWJ sequence of such pictographs (families, professions, flags such
//     as the rainbow flag);
//   - a keycap sequence ([0-9#*] U+FE0F? U+20E3);
//   - a flag: two regional indicators;
//   - a tag sequence (subdivision flags: U+1F3F4 + tag characters).
//
// Letters, digits, punctuation, whitespace, combining marks, text-presentation
// selectors (U+FE0E), lone ZWJ / selectors / modifiers / regional indicators
// are rejected. A pictograph whose default presentation is text (©, ☺, ❤ ...)
// is accepted and normalized to its emoji presentation by inserting U+FE0F,
// so the stored tag renders as emoji everywhere.
//
// The tables below are the Extended_Pictographic and Emoji_Presentation
// properties from Unicode emoji-data (the reserved 1FC00–1FFFD block, which
// has no assigned characters, is left out so an unassigned code point cannot
// pass as an emoji).
package emoji

import (
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/rivo/uniseg"
	"golang.org/x/text/unicode/norm"
)

const (
	// MinTagEmoji and MaxTagEmoji bound the number of emoji in a tag.
	MinTagEmoji = 1
	MaxTagEmoji = 6
)

var (
	// ErrEmpty: the tag has no emoji (empty or whitespace only).
	ErrEmpty = errors.New("emoji: add at least one emoji")
	// ErrTooMany: more than MaxTagEmoji emoji.
	ErrTooMany = errors.New("emoji: use at most 6 emoji")
	// ErrNotEmoji: a character that is not an emoji.
	ErrNotEmoji = errors.New("emoji: only emoji are allowed")
)

const (
	runeZwj              = 0x200D
	runeVs15             = 0xFE0E
	runeVs16             = 0xFE0F
	runeKeycap           = 0x20E3
	runeBlackFlag        = 0x1F3F4
	runeTagStart         = 0xE0020
	runeTagEnd           = 0xE007E
	runeTagCancel        = 0xE007F
	runeModifierStart    = 0x1F3FB
	runeModifierEnd      = 0x1F3FF
	runeRegionalIndStart = 0x1F1E6
	runeRegionalIndEnd   = 0x1F1FF
)

// ValidateTag checks that s is 1–6 emoji and nothing else. It returns the
// NFC-normalized tag with text-default pictographs promoted to emoji
// presentation, the number of emoji in it, and nil; or "", 0 and one of
// ErrEmpty, ErrTooMany, ErrNotEmoji.
func ValidateTag(s string) (normalized string, count int, err error) {
	s = norm.NFC.String(strings.TrimSpace(s))
	if s == "" {
		return "", 0, ErrEmpty
	}
	if !utf8.ValidString(s) {
		return "", 0, ErrNotEmoji
	}

	var out strings.Builder
	graphemes := uniseg.NewGraphemes(s)
	for graphemes.Next() {
		cluster := graphemes.Str()
		normalizedCluster, ok := normalizeCluster(cluster)
		if !ok {
			return "", 0, ErrNotEmoji
		}
		count += 1
		if MaxTagEmoji < count {
			return "", 0, ErrTooMany
		}
		out.WriteString(normalizedCluster)
	}
	if count < MinTagEmoji {
		return "", 0, ErrEmpty
	}
	return out.String(), count, nil
}

// IsTag reports whether s is a valid tag.
func IsTag(s string) bool {
	_, _, err := ValidateTag(s)
	return err == nil
}

// Count returns the number of emoji in a valid tag, or 0.
func Count(s string) int {
	_, count, err := ValidateTag(s)
	if err != nil {
		return 0
	}
	return count
}

// normalizeCluster validates one grapheme cluster as a single emoji and
// returns its normalized form.
func normalizeCluster(cluster string) (string, bool) {
	runes := []rune(cluster)
	if len(runes) == 0 {
		return "", false
	}

	if s, ok := keycapSequence(runes); ok {
		return s, true
	}
	if s, ok := flagSequence(runes); ok {
		return s, true
	}
	if s, ok := tagSequence(runes); ok {
		return s, true
	}
	return zwjSequence(runes)
}

// keycapSequence: [0-9#*] U+FE0F? U+20E3, normalized with U+FE0F.
func keycapSequence(runes []rune) (string, bool) {
	if len(runes) < 2 || len(runes) > 3 {
		return "", false
	}
	base := runes[0]
	if !(base == '#' || base == '*' || ('0' <= base && base <= '9')) {
		return "", false
	}
	if len(runes) == 3 {
		if runes[1] != runeVs16 || runes[2] != runeKeycap {
			return "", false
		}
	} else if runes[1] != runeKeycap {
		return "", false
	}
	return string([]rune{base, runeVs16, runeKeycap}), true
}

// flagSequence: exactly two regional indicators.
func flagSequence(runes []rune) (string, bool) {
	if len(runes) != 2 {
		return "", false
	}
	for _, r := range runes {
		if r < runeRegionalIndStart || runeRegionalIndEnd < r {
			return "", false
		}
	}
	return string(runes), true
}

// tagSequence: U+1F3F4 followed by tag characters and the tag cancel
// (subdivision flags such as England, Scotland, Wales).
func tagSequence(runes []rune) (string, bool) {
	if len(runes) < 3 || runes[0] != runeBlackFlag {
		return "", false
	}
	if runes[len(runes)-1] != runeTagCancel {
		return "", false
	}
	for _, r := range runes[1 : len(runes)-1] {
		if r < runeTagStart || runeTagEnd < r {
			return "", false
		}
	}
	return string(runes), true
}

// zwjSequence: element (U+200D element)*, where an element is a pictograph
// with an optional U+FE0F and an optional skin tone modifier. Text-default
// pictographs without a modifier get U+FE0F inserted.
func zwjSequence(runes []rune) (string, bool) {
	var out []rune
	i := 0
	for {
		next, ok := zwjElement(runes, i, &out)
		if !ok {
			return "", false
		}
		i = next
		if i == len(runes) {
			return string(out), true
		}
		if runes[i] != runeZwj {
			return "", false
		}
		out = append(out, runeZwj)
		i += 1
		if i == len(runes) {
			// trailing joiner
			return "", false
		}
	}
}

// zwjElement consumes one element starting at runes[i], appends its
// normalized form to out and returns the next index.
func zwjElement(runes []rune, i int, out *[]rune) (int, bool) {
	base := runes[i]
	if !IsExtendedPictographic(base) {
		return i, false
	}
	i += 1
	*out = append(*out, base)
	hasVs16 := false
	if i < len(runes) {
		switch runes[i] {
		case runeVs16:
			hasVs16 = true
			i += 1
		case runeVs15:
			// text presentation: not an emoji
			return i, false
		}
	}
	hasModifier := false
	if i < len(runes) && runeModifierStart <= runes[i] && runes[i] <= runeModifierEnd {
		hasModifier = true
	}
	if hasVs16 || (!hasModifier && !IsEmojiPresentation(base)) {
		*out = append(*out, runeVs16)
	}
	if hasModifier {
		*out = append(*out, runes[i])
		i += 1
	}
	return i, true
}

type runeRange struct {
	lo rune
	hi rune
}

func inRanges(r rune, ranges []runeRange) bool {
	// ranges are sorted; binary search
	lo, hi := 0, len(ranges)
	for lo < hi {
		mid := (lo + hi) / 2
		if r < ranges[mid].lo {
			hi = mid
		} else if ranges[mid].hi < r {
			lo = mid + 1
		} else {
			return true
		}
	}
	return false
}

// IsExtendedPictographic reports whether r has the Extended_Pictographic
// property (a pictograph that can be an emoji).
func IsExtendedPictographic(r rune) bool {
	return inRanges(r, extendedPictographic)
}

// IsEmojiPresentation reports whether r renders as emoji by default
// (Emoji_Presentation). A pictograph without it needs U+FE0F to be an emoji.
func IsEmojiPresentation(r rune) bool {
	return inRanges(r, emojiPresentation)
}

// Extended_Pictographic, Unicode emoji-data, sorted.
var extendedPictographic = []runeRange{
	{0x00A9, 0x00A9}, {0x00AE, 0x00AE}, {0x203C, 0x203C}, {0x2049, 0x2049},
	{0x2122, 0x2122}, {0x2139, 0x2139}, {0x2194, 0x2199}, {0x21A9, 0x21AA},
	{0x231A, 0x231B}, {0x2328, 0x2328}, {0x2388, 0x2388}, {0x23CF, 0x23CF},
	{0x23E9, 0x23F3}, {0x23F8, 0x23FA}, {0x24C2, 0x24C2}, {0x25AA, 0x25AB},
	{0x25B6, 0x25B6}, {0x25C0, 0x25C0}, {0x25FB, 0x25FE}, {0x2600, 0x2605},
	{0x2607, 0x2612}, {0x2614, 0x2685}, {0x2690, 0x2705}, {0x2708, 0x2712},
	{0x2714, 0x2714}, {0x2716, 0x2716}, {0x271D, 0x271D}, {0x2721, 0x2721},
	{0x2728, 0x2728}, {0x2733, 0x2734}, {0x2744, 0x2744}, {0x2747, 0x2747},
	{0x274C, 0x274C}, {0x274E, 0x274E}, {0x2753, 0x2755}, {0x2757, 0x2757},
	{0x2763, 0x2767}, {0x2795, 0x2797}, {0x27A1, 0x27A1}, {0x27B0, 0x27B0},
	{0x27BF, 0x27BF}, {0x2934, 0x2935}, {0x2B05, 0x2B07}, {0x2B1B, 0x2B1C},
	{0x2B50, 0x2B50}, {0x2B55, 0x2B55}, {0x3030, 0x3030}, {0x303D, 0x303D},
	{0x3297, 0x3297}, {0x3299, 0x3299},
	{0x1F000, 0x1F0FF}, {0x1F10D, 0x1F10F}, {0x1F12F, 0x1F12F}, {0x1F16C, 0x1F171},
	{0x1F17E, 0x1F17F}, {0x1F18E, 0x1F18E}, {0x1F191, 0x1F19A}, {0x1F1AD, 0x1F1E5},
	{0x1F201, 0x1F20F}, {0x1F21A, 0x1F21A}, {0x1F22F, 0x1F22F}, {0x1F232, 0x1F23A},
	{0x1F23C, 0x1F23F}, {0x1F249, 0x1F3FA}, {0x1F400, 0x1F53D}, {0x1F546, 0x1F64F},
	{0x1F680, 0x1F6FF}, {0x1F774, 0x1F77F}, {0x1F7D5, 0x1F7FF}, {0x1F80C, 0x1F80F},
	{0x1F848, 0x1F84F}, {0x1F85A, 0x1F85F}, {0x1F888, 0x1F88F}, {0x1F8AE, 0x1F8FF},
	{0x1F90C, 0x1F93A}, {0x1F93C, 0x1F945}, {0x1F947, 0x1FAFF},
}

// Emoji_Presentation, Unicode emoji-data, sorted.
var emojiPresentation = []runeRange{
	{0x231A, 0x231B}, {0x23E9, 0x23EC}, {0x23F0, 0x23F0}, {0x23F3, 0x23F3},
	{0x25FD, 0x25FE}, {0x2614, 0x2615}, {0x2648, 0x2653}, {0x267F, 0x267F},
	{0x2693, 0x2693}, {0x26A1, 0x26A1}, {0x26AA, 0x26AB}, {0x26BD, 0x26BE},
	{0x26C4, 0x26C5}, {0x26CE, 0x26CE}, {0x26D4, 0x26D4}, {0x26EA, 0x26EA},
	{0x26F2, 0x26F3}, {0x26F5, 0x26F5}, {0x26FA, 0x26FA}, {0x26FD, 0x26FD},
	{0x2705, 0x2705}, {0x270A, 0x270B}, {0x2728, 0x2728}, {0x274C, 0x274C},
	{0x274E, 0x274E}, {0x2753, 0x2755}, {0x2757, 0x2757}, {0x2795, 0x2797},
	{0x27B0, 0x27B0}, {0x27BF, 0x27BF}, {0x2B1B, 0x2B1C}, {0x2B50, 0x2B50},
	{0x2B55, 0x2B55},
	{0x1F004, 0x1F004}, {0x1F0CF, 0x1F0CF}, {0x1F18E, 0x1F18E}, {0x1F191, 0x1F19A},
	{0x1F1E6, 0x1F1FF}, {0x1F201, 0x1F201}, {0x1F21A, 0x1F21A}, {0x1F22F, 0x1F22F},
	{0x1F232, 0x1F236}, {0x1F238, 0x1F23A}, {0x1F250, 0x1F251}, {0x1F300, 0x1F320},
	{0x1F32D, 0x1F335}, {0x1F337, 0x1F37C}, {0x1F37E, 0x1F393}, {0x1F3A0, 0x1F3CA},
	{0x1F3CF, 0x1F3D3}, {0x1F3E0, 0x1F3F0}, {0x1F3F4, 0x1F3F4}, {0x1F3F8, 0x1F43E},
	{0x1F440, 0x1F440}, {0x1F442, 0x1F4FC}, {0x1F4FF, 0x1F53D}, {0x1F54B, 0x1F54E},
	{0x1F550, 0x1F567}, {0x1F57A, 0x1F57A}, {0x1F595, 0x1F596}, {0x1F5A4, 0x1F5A4},
	{0x1F5FB, 0x1F64F}, {0x1F680, 0x1F6C5}, {0x1F6CC, 0x1F6CC}, {0x1F6D0, 0x1F6D2},
	{0x1F6D5, 0x1F6D7}, {0x1F6DC, 0x1F6DF}, {0x1F6EB, 0x1F6EC}, {0x1F6F4, 0x1F6FC},
	{0x1F7E0, 0x1F7EB}, {0x1F7F0, 0x1F7F0}, {0x1F90C, 0x1F93A}, {0x1F93C, 0x1F945},
	{0x1F947, 0x1F9FF}, {0x1FA70, 0x1FA7C}, {0x1FA80, 0x1FA89}, {0x1FA8F, 0x1FA8F},
	{0x1FA90, 0x1FABE}, {0x1FABF, 0x1FAC6}, {0x1FACE, 0x1FADC}, {0x1FADF, 0x1FAE9},
	{0x1FAF0, 0x1FAF8},
}
