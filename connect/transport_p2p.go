



// transport implementation with webrtc
// halving ramp up prioritization as long as the route is sending data at the fraction requested

// a + (1 - a) / 2 = b
// a  + 1/2 - a/2 = b
// a * (1 - 1/2) + 1/2 = b
// a = 2 * (b - 1/2)

// e error
// a + (1 - a) / 2 = 1 - e
// second step
// (a + (1 - a) / 2) + (1 - (a + (1 - a) / 2)) / 2

// n steps
// (2^n - 1 + a)/(2^n)

// Log_2[(1-a)/e]
