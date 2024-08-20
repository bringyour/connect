

## Distributed traffic protocol

For a traffic pair (src ip, src port, dst ip, dst port), 

find an egress hop to give the best app experience

sender (you) -> [bring your network -> egress hop] -> 

sender -> [               egress hop1 (estimated speed, estimated capacity?)]
          [               egress hop2]
          [               egress hop3]
          [               egress hop4]
          ... inf or very large

random enumeration
get N hops not seen before
freshness of data

each element a unique random path (linpath, min latency, give it ping times shared set of servers and it gives back N paths, use ping times to choose closest dests also)
sender -> {... [              egress hopA (estimated speed)]  600mib
           ... [              egress hopB] 10mib
           ... [              egress hopC] 60mib
           ... [              egress hopD] 1mib
           ... [              egress hopE] 0
           ]}

- bad hop
- blocked hop
- disappeared hop

attack where someone forces your first hop


(src ip, src port, dst ip, dst port, proto)
choose one hop per tuple

on the sender
statistical with hop window management
- distribute traffic pairs over N egress hops statistically (hop window)
- weighted by data transferred in the past K seconds
  when a hop has not been tried, it is considered to be an initial estimate for weighting
  initial estimate can be min(estimated speed, estimated capacity)?
  dual weighting, (a) by all data, (b) by data to (dst ip, dest port)
- if >R connections to (dst ip, dst port) have been tried in the past K secxonds, 
  expand the hop window size +S
  windowSize should be at least base+((number of connections)/R)*S
  use an L second sequence window
- every K seconds, if the hop window size is above the baseline N,
  remove one hop that has routed the least data in the past K seconds



packet interval window
record first and last time for tuple, plus data count
use speed as the weight (interval data/interval time)

packet interval window sequence
maintains a larger window broken into smaller windows
ask questions of the larger window and it uses the smaller windows

groups
order ends before starts if end is within M seconds of start





Create a simulation
Sender Stream has timeout of received N bytes in the last K seconds
Intermediaries and egress have throughput limits, and randomly blackhole certain dest
count number of retries for a connection, data for a connection, and total time to transfer the connection.

Compare to the expected number of retries given a random uniform selection and 1/P chance of block at each egress




bucket and choose a random choice in bucket

(src ip A, src port A, dst ip A, dst port A, proto)
choose hopA
# not routing!
# refresh/reset
(src ip A, src port B, dst ip A, dst port A, proto)
choose hopB




# send (dst ip, dst port) to the egress hop 


## Privacy additions

- Provable anonymity
- Enforce secure DNS


## Addional

- Regions (content)
- Access the internet uniformly



## Marketplace

- Fix contract handling
- Fix payouts




Privacy Hub
- a single home for your privacy documents, privacy.yourdomain.com
- quickly generate a new privacy policy and update your privacy policy to be compliant with local laws
- one place for user data requests
- allows easy compliance like opt out, data share requests, via web ui and api that integrates with your api and ticket systems
- can pull data from real users as part of the Network Marketplace to audit user to cloud data flows for your product
- integrate with GitHub and build tools to make sure data flows are documented in the privacy policy before launch




