package emoji

import (
	"math/rand"
	"strings"
)

// SuggestMaxEmoji is the most emoji Suggest returns. A suggestion is a short
// starting point the user edits, not a full six-emoji tag.
const SuggestMaxEmoji = 3

// Suggest returns count distinct emoji from the suggestion pool, joined into
// a tag that ValidateTag accepts. count is clamped to 1..SuggestMaxEmoji; a
// count of zero or less picks the length at random in that range. A nil r
// uses the package-level random source (seeded by the runtime).
//
// The pool is a curated subset of the single-code-point emoji whose default
// presentation is already emoji (no U+FE0F needed), so a suggestion renders
// the same on every platform's default emoji font: animals, plants, weather,
// food, objects, sports and symbols. No flags, skin tones, ZWJ sequences,
// faces or anything with an ambiguous meaning (a broken heart, weapons,
// alcohol, medicine).
func Suggest(count int, r *rand.Rand) string {
	intn := rand.Intn
	perm := rand.Perm
	if r != nil {
		intn = r.Intn
		perm = r.Perm
	}
	switch {
	case count <= 0:
		count = 1 + intn(SuggestMaxEmoji)
	case SuggestMaxEmoji < count:
		count = SuggestMaxEmoji
	}
	order := perm(len(suggestPool))
	var tag strings.Builder
	for i := 0; i < count; i++ {
		tag.WriteRune(suggestPool[order[i]])
	}
	return tag.String()
}

// SuggestPoolSize is the number of emoji Suggest can pick from.
func SuggestPoolSize() int {
	return len(suggestPool)
}

// suggestPool: every entry is a single Emoji_Presentation code point (the
// test checks each against the table and ValidateTag). Emoji 11.0 (2018) or
// older only, so fonts on the oldest supported phones have every glyph.
var suggestPool = []rune{
	// animals
	0x1F436, // 🐶 dog face
	0x1F431, // 🐱 cat face
	0x1F42D, // 🐭 mouse face
	0x1F439, // 🐹 hamster
	0x1F430, // 🐰 rabbit face
	0x1F98A, // 🦊 fox
	0x1F43B, // 🐻 bear
	0x1F43C, // 🐼 panda
	0x1F428, // 🐨 koala
	0x1F42F, // 🐯 tiger face
	0x1F981, // 🦁 lion
	0x1F42E, // 🐮 cow face
	0x1F437, // 🐷 pig face
	0x1F438, // 🐸 frog
	0x1F435, // 🐵 monkey face
	0x1F414, // 🐔 chicken
	0x1F427, // 🐧 penguin
	0x1F426, // 🐦 bird
	0x1F424, // 🐤 baby chick
	0x1F986, // 🦆 duck
	0x1F985, // 🦅 eagle
	0x1F989, // 🦉 owl
	0x1F987, // 🦇 bat
	0x1F43A, // 🐺 wolf
	0x1F417, // 🐗 boar
	0x1F434, // 🐴 horse face
	0x1F984, // 🦄 unicorn
	0x1F41D, // 🐝 honeybee
	0x1F41B, // 🐛 bug
	0x1F98B, // 🦋 butterfly
	0x1F40C, // 🐌 snail
	0x1F41E, // 🐞 lady beetle
	0x1F422, // 🐢 turtle
	0x1F40D, // 🐍 snake
	0x1F98E, // 🦎 lizard
	0x1F419, // 🐙 octopus
	0x1F991, // 🦑 squid
	0x1F990, // 🦐 shrimp
	0x1F980, // 🦀 crab
	0x1F421, // 🐡 blowfish
	0x1F420, // 🐠 tropical fish
	0x1F41F, // 🐟 fish
	0x1F42C, // 🐬 dolphin
	0x1F433, // 🐳 spouting whale
	0x1F40B, // 🐋 whale
	0x1F988, // 🦈 shark
	0x1F40A, // 🐊 crocodile
	0x1F405, // 🐅 tiger
	0x1F406, // 🐆 leopard
	0x1F993, // 🦓 zebra
	0x1F98D, // 🦍 gorilla
	0x1F418, // 🐘 elephant
	0x1F98F, // 🦏 rhinoceros
	0x1F42A, // 🐪 camel
	0x1F992, // 🦒 giraffe
	0x1F998, // 🦘 kangaroo
	0x1F40F, // 🐏 ram
	0x1F410, // 🐐 goat
	0x1F98C, // 🦌 deer
	0x1F415, // 🐕 dog
	0x1F408, // 🐈 cat
	0x1F413, // 🐓 rooster
	0x1F99C, // 🦜 parrot
	0x1F99A, // 🦚 peacock
	0x1F994, // 🦔 hedgehog
	0x1F996, // 🦖 t-rex
	0x1F995, // 🦕 sauropod
	0x1F409, // 🐉 dragon
	0x1F432, // 🐲 dragon face
	// plants, weather, space
	0x1F335, // 🌵 cactus
	0x1F384, // 🎄 christmas tree
	0x1F332, // 🌲 evergreen tree
	0x1F333, // 🌳 deciduous tree
	0x1F334, // 🌴 palm tree
	0x1F331, // 🌱 seedling
	0x1F33F, // 🌿 herb
	0x1F340, // 🍀 four leaf clover
	0x1F341, // 🍁 maple leaf
	0x1F342, // 🍂 fallen leaf
	0x1F343, // 🍃 leaf fluttering in wind
	0x1F344, // 🍄 mushroom
	0x1F330, // 🌰 chestnut
	0x1F33E, // 🌾 sheaf of rice
	0x1F337, // 🌷 tulip
	0x1F339, // 🌹 rose
	0x1F33A, // 🌺 hibiscus
	0x1F338, // 🌸 cherry blossom
	0x1F33C, // 🌼 blossom
	0x1F33B, // 🌻 sunflower
	0x1F31E, // 🌞 sun with face
	0x1F31D, // 🌝 full moon face
	0x1F31B, // 🌛 first quarter moon face
	0x1F319, // 🌙 crescent moon
	0x1F30E, // 🌎 globe americas
	0x1F30D, // 🌍 globe europe-africa
	0x1F30F, // 🌏 globe asia-australia
	0x1F4AB, // 💫 dizzy
	0x2B50,  // ⭐ star
	0x1F31F, // 🌟 glowing star
	0x2728,  // ✨ sparkles
	0x26A1,  // ⚡ high voltage
	0x1F525, // 🔥 fire
	0x1F308, // 🌈 rainbow
	0x26C5,  // ⛅ sun behind cloud
	0x26C4,  // ⛄ snowman
	0x1F30A, // 🌊 water wave
	0x1F4A7, // 💧 droplet
	0x1F30B, // 🌋 volcano
	0x1F5FB, // 🗻 mount fuji
	// food
	0x1F34E, // 🍎 red apple
	0x1F350, // 🍐 pear
	0x1F34A, // 🍊 tangerine
	0x1F34B, // 🍋 lemon
	0x1F34C, // 🍌 banana
	0x1F349, // 🍉 watermelon
	0x1F347, // 🍇 grapes
	0x1F353, // 🍓 strawberry
	0x1F352, // 🍒 cherries
	0x1F351, // 🍑 peach
	0x1F34D, // 🍍 pineapple
	0x1F965, // 🥥 coconut
	0x1F95D, // 🥝 kiwi fruit
	0x1F345, // 🍅 tomato
	0x1F951, // 🥑 avocado
	0x1F966, // 🥦 broccoli
	0x1F955, // 🥕 carrot
	0x1F33D, // 🌽 ear of corn
	0x1F950, // 🥐 croissant
	0x1F968, // 🥨 pretzel
	0x1F9C0, // 🧀 cheese wedge
	0x1F354, // 🍔 hamburger
	0x1F35F, // 🍟 french fries
	0x1F355, // 🍕 pizza
	0x1F32E, // 🌮 taco
	0x1F363, // 🍣 sushi
	0x1F35C, // 🍜 steaming bowl
	0x1F369, // 🍩 doughnut
	0x1F36A, // 🍪 cookie
	0x1F382, // 🎂 birthday cake
	0x1F370, // 🍰 shortcake
	0x1F36B, // 🍫 chocolate bar
	0x1F37F, // 🍿 popcorn
	0x1F36F, // 🍯 honey pot
	0x2615,  // ☕ hot beverage
	0x1F375, // 🍵 teacup
	0x1F366, // 🍦 soft ice cream
	0x1F36D, // 🍭 lollipop
	0x1F36C, // 🍬 candy
	0x1F95E, // 🥞 pancakes
	// sports and games
	0x26BD,  // ⚽ soccer ball
	0x1F3C0, // 🏀 basketball
	0x1F3C8, // 🏈 american football
	0x26BE,  // ⚾ baseball
	0x1F3BE, // 🎾 tennis
	0x1F3D0, // 🏐 volleyball
	0x1F3C9, // 🏉 rugby football
	0x1F3B1, // 🎱 pool 8 ball
	0x1F3D3, // 🏓 ping pong
	0x1F3F8, // 🏸 badminton
	0x26F3,  // ⛳ flag in hole
	0x1F3AF, // 🎯 bullseye
	0x1F3AE, // 🎮 video game
	0x1F3B2, // 🎲 game die
	0x1F3B8, // 🎸 guitar
	0x1F3B9, // 🎹 musical keyboard
	0x1F3BA, // 🎺 trumpet
	0x1F3BB, // 🎻 violin
	0x1F941, // 🥁 drum
	0x1F3A7, // 🎧 headphone
	0x1F3A4, // 🎤 microphone
	0x1F3A8, // 🎨 artist palette
	0x1F3AC, // 🎬 clapper board
	0x1F3AD, // 🎭 performing arts
	0x1F3AA, // 🎪 circus tent
	0x1F3A1, // 🎡 ferris wheel
	0x1F3A2, // 🎢 roller coaster
	0x1F3A0, // 🎠 carousel horse
	0x1F3C6, // 🏆 trophy
	0x1F947, // 🥇 first place medal
	0x1F3C5, // 🏅 sports medal
	// travel and places
	0x1F680, // 🚀 rocket
	0x1F6F8, // 🛸 flying saucer
	0x1F681, // 🚁 helicopter
	0x26F5,  // ⛵ sailboat
	0x1F682, // 🚂 locomotive
	0x1F697, // 🚗 automobile
	0x1F695, // 🚕 taxi
	0x1F699, // 🚙 sport utility vehicle
	0x1F68C, // 🚌 bus
	0x1F6B2, // 🚲 bicycle
	0x1F6F4, // 🛴 kick scooter
	0x1F6A4, // 🚤 speedboat
	0x2693,  // ⚓ anchor
	0x1F3F0, // 🏰 castle
	0x1F5FC, // 🗼 tokyo tower
	0x1F5FD, // 🗽 statue of liberty
	0x1F3E0, // 🏠 house
	0x1F3EE, // 🏮 red paper lantern
	// objects and symbols
	0x1F9ED, // 🧭 compass
	0x1F9F2, // 🧲 magnet
	0x1F52E, // 🔮 crystal ball
	0x1F388, // 🎈 balloon
	0x1F381, // 🎁 wrapped gift
	0x1F380, // 🎀 ribbon
	0x1F389, // 🎉 party popper
	0x1F38A, // 🎊 confetti ball
	0x1F48E, // 💎 gem stone
	0x1F451, // 👑 crown
	0x1F3A9, // 🎩 top hat
	0x1F9E2, // 🧢 billed cap
	0x1F453, // 👓 glasses
	0x1F511, // 🔑 key
	0x1F514, // 🔔 bell
	0x1F4DA, // 📚 books
	0x1F4D6, // 📖 open book
	0x1F4CC, // 📌 pushpin
	0x1F4A1, // 💡 light bulb
	0x1F526, // 🔦 flashlight
	0x1F50B, // 🔋 battery
	0x1F50C, // 🔌 electric plug
	0x1F4BB, // 💻 laptop
	0x1F4F1, // 📱 mobile phone
	0x231A,  // ⌚ watch
	0x23F0,  // ⏰ alarm clock
	0x23F3,  // ⏳ hourglass not done
	0x1F4F7, // 📷 camera
	0x1F3A5, // 🎥 movie camera
	0x1F4FA, // 📺 television
	0x1F4FB, // 📻 radio
	0x1F9E9, // 🧩 puzzle piece
	0x1F9F8, // 🧸 teddy bear
	0x1F38F, // 🎏 carp streamer
	0x1F390, // 🎐 wind chime
	0x1F38B, // 🎋 tanabata tree
	0x1F383, // 🎃 jack-o-lantern
	0x1F47B, // 👻 ghost
	0x1F916, // 🤖 robot
	0x1F47D, // 👽 alien
	0x1F47E, // 👾 alien monster
	0x1F499, // 💙 blue heart
	0x1F49A, // 💚 green heart
	0x1F49B, // 💛 yellow heart
	0x1F49C, // 💜 purple heart
	0x1F9E1, // 🧡 orange heart
	0x1F5A4, // 🖤 black heart
	0x1F496, // 💖 sparkling heart
	0x2705,  // ✅ check mark button
	0x1F506, // 🔆 bright button
	0x1F531, // 🔱 trident emblem
	0x1F530, // 🔰 japanese symbol for beginner
	0x1F4A0, // 💠 diamond with a dot
	0x1F536, // 🔶 large orange diamond
	0x1F537, // 🔷 large blue diamond
	0x1F535, // 🔵 blue circle
	0x26AB,  // ⚫ black circle
	0x26AA,  // ⚪ white circle
	0x1F4AF, // 💯 hundred points
	0x1F192, // 🆒 cool button
	0x1F195, // 🆕 new button
	0x1F199, // 🆙 up button
	0x1F51D, // 🔝 top arrow
	0x1F3B5, // 🎵 musical note
	0x1F3B6, // 🎶 musical notes
	0x1F50A, // 🔊 speaker high volume
	0x1F4E3, // 📣 megaphone
	0x1F6A9, // 🚩 triangular flag
	0x1F3C1, // 🏁 chequered flag
	0x1F38C, // 🎌 crossed flags
}
