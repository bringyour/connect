package connect

// linpath is a linear path in terms of latency from source to destination with multiple hops
// the destination is chosen using a destinationcriteria
// the generator can generate multiple alternate paths to the same destination

// New(criteria, number of hops)
// CreateDestination(alternateCount int)
// CreateAlternates([]Path currentPaths, destinationId, alternateCount int)
// takedestination() []Path
// takeAlternates(destinationId) []Path





// FIXME latencyclient
// FIXME after each measurement, send an update to the control
// FIXME ping both ipv4 and ipv6
// FIXME a client that has ipv6 support can only select providers that have ipv6 ping results
// FIXME the platform determines a client has ipv6 support depending on whether it has ipv6 ping results

// FIXME all clients are enabled with ipv4 support
// FIXME option to enable ipv6 but that limits the number of matching providers



