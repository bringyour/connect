let CLIENT2VIS_EXPORT = "export-low.json";

let packets;
let events;

let packetHeadIndex;
let eventHeadIndex;

let egressEndTimeOffsets;
let senderStartTimeOffsets;
let senderEndTimeOffsets;

let egressIds;
let senderIds;
// egressId -> dst -> bool
let egressBlocks;
// egressId -> bool
let egressDrops;

// sequenceId -> progress
let seqMaxProgress;

let minPacketWindowMillis = 200;
let transitionOutMillis = 2000;
let maxRadius = 12;
let textPadding = 4;
let sketchPadding = 24;
let marginLeft = 30;
let marginRight = 100;
let marginTop = 30;
let headerTextSize = 14;


let resetTimeOffsetMillis = 0;




function setup() {
  createCanvas(768, 2048);
  
  loadJSON(CLIENT2VIS_EXPORT, dataLoaded);
}


function dataLoaded(data) {
  packets = data.packets;
  events = data.events;
  
  reset();

  // `packets` and `events` are ordered by `event_time_offset` ascending
  // process events to find end times for sender and egress
  // sender-end
  // contract
  
  egressEndTimeOffsets = {};
  senderStartTimeOffsets = {};
  senderEndTimeOffsets = {};
  
  for (const event of events) {
    switch (event.event_type) {
      case "contract":
        egressEndTimeOffsets[event.client_id] = event.event_time_offset_millis;
        break;
      case "sender-start":
        senderStartTimeOffsets[event.client_id] = event.event_time_offset_millis;
        break
      case "sender-end":
        senderEndTimeOffsets[event.client_id] = event.event_time_offset_millis;
        break
    }
  }
}



// for a time range [a, b)
// render all senders active in that time range (using the src ips from packets)
// render all egress active in that time range (using the egress list)
// render lines between sender and egress in that time range
// render active block and drop events on each egress


// packet
// binary search for start time
// binary search for end time
// between these two indexes are the packets


// width of line encodes number of packets
// color of line encodes blocked or dropped at dest (dropped red, blocked orange)

// label line with dest info and max packet index
// label egress with egressId
// dropped egress red
// annotate egress with block and drop info

function draw() {
  if (!packets || !events) {
    // loading
    return
  }
  
  background(color(255, 255, 255));
  
  let timeOffsetMillis = millis() - resetTimeOffsetMillis;
  let packetTailIndex = packetHeadIndex;
  let eventTailIndex = eventHeadIndex;
  
  // forward the indexes
  while (packetHeadIndex < packets.length && packets[packetHeadIndex].event_time_offset_millis < timeOffsetMillis) {
    packetHeadIndex += 1;
  }
  
  // move the packet tail back to meet the `minWindowMillis`
  while (0 < packetTailIndex && timeOffsetMillis - packets[packetTailIndex - 1].event_time_offset_millis < minPacketWindowMillis) {
    packetTailIndex -= 1;
  }
  
  
  
  while (eventHeadIndex < events.length && events[eventHeadIndex].event_time_offset_millis < timeOffsetMillis) {
    eventHeadIndex += 1;
  }
  
  
  
  
  //console.log(`[${packetTailIndex}, ${packetHeadIndex}) ${packets.length}`);
  //console.log(`[${eventTailIndex}, ${eventHeadIndex}) ${events.length}`);
  
  // senderId -> egressId -> dst -> fraction
  let seqProgress = {};
  
  for (let i = packetTailIndex; i < packetHeadIndex; i += 1) {
    const packet = packets[i];
    seqMaxProgress[packet.src_client_id] = 0;
  }
  
  for (let i = packetTailIndex; i < packetHeadIndex; i += 1) {
    const packet = packets[i];
    const dst = dstStr(packet.connection_tuple);
    let egressDstProgress = seqProgress[packet.src_client_id];
    if (egressDstProgress === undefined) {
      egressDstProgress = {};
      seqProgress[packet.src_client_id] = egressDstProgress;
    }
    let dstProgress = egressDstProgress[packet.dst_client_id];
    if (dstProgress === undefined) {
      dstProgress = {};
      egressDstProgress[packet.dst_client_id] = dstProgress;
    }
    let progress = packet.index / packet.seq_size;
    dstProgress[dst] = progress;
    seqMaxProgress[packet.src_client_id] = max(seqMaxProgress[packet.src_client_id], progress);
  }
  
  for (let i = eventTailIndex; i < eventHeadIndex; i += 1) {
    const event = events[i];
    // sender-start
    // sender-end
    // expand
    // contract
    // drop-start
    // drop-end
    // block-start
    // block-end
    switch (event.event_type) {
      case "sender-start": {
        //console.log(`SENDER START ${event.client_id}`);
          senderIds.push(event.client_id);
        }
        break
      case "sender-end": {
          const index = senderIds.indexOf(event.client_id);
          //console.log(`SENDER END ${event.client_id}`);
          if (0 <= index) {
            senderIds.splice(index, 1);
          }
        }
        break
      case "expand": {
          egressIds.push(event.client_id);
        }
        break
      case "contract": {
          const index = egressIds.indexOf(event.client_id);
          if (0 <= index) {
            egressIds.splice(index, 1);
          }
        }
        break
      case "block-start": {
          const dst = dstStr(event.dst)
          //console.log(`BLOCK START ${event.client_id} ${dst}`);
          let blocks = egressBlocks[event.client_id];
          if (blocks === undefined) {
            blocks = {};
            egressBlocks[event.client_id] = blocks;
          }
          blocks[dst] = true;
        }
        break
      case "block-end": {
          const dst = dstStr(event.dst)
          const blocks = egressBlocks[event.client_id];
          if (blocks !== undefined) {
            delete blocks[dst];
            if (blocks.length == 0) {
              delete egressBlocks[event.client_id];
            }
          }
        }
        break
      case "drop-start": {
          egressDrops[event.client_id] = true;
        }
        break
      case "drop-end": {
          delete egressDrops[event.client_id];
        }
        break;
    }
  }
  
  // process events [eventTailIndex, eventHeadIndex)
  // UPDATE egressWindow
  // UPDATE egressBlocks
  // UPDATE egressDrops
  
  
  // process packets [packetTailIndex, packetHeadIndex)
  // TODO form connections from sender to egress, and stats for each connection
  
  
  // 2 * (r + textPadding) * s = h
  let r = min((height - 2 * sketchPadding) / (2 * max(senderIds.length, egressIds.length)) - textPadding, maxRadius);
  
    // senderId -> y
  let senderX = sketchPadding + r + textPadding + marginLeft;
  let senderYs = {};
  let egressX = width - (sketchPadding + r + textPadding + marginRight);
  // egressId -> y
  let egressYs = {};
  
  // layout `senderY` and `egressY`
  let senderY = sketchPadding + marginTop + r + textPadding;
  for (const senderId of senderIds) {
    senderYs[senderId] = senderY;
    senderY += 2 * (r + textPadding);
  }
  
  let egressY = sketchPadding + marginTop + r + textPadding;
  for (const egressId of egressIds) {
    egressYs[egressId] = egressY;
    egressY += 2 * (r + textPadding);
  }
  
  
  // render
  
  
  textSize(headerTextSize);
  textAlign(CENTER, BOTTOM);
  fill(color(0, 0, 0));
  text("Connections", senderX, sketchPadding + marginTop - 2 * headerTextSize - textPadding);
  
  textAlign(CENTER, BOTTOM);
  fill(color(0, 0, 0));
  text("Egress Window (linpaths)", egressX, sketchPadding + marginTop - 2 * headerTextSize - textPadding);
  
  textAlign(LEFT, BOTTOM);
  fill(color(0, 0, 0));
  text("Egress hidden state", egressX + r + textPadding, sketchPadding + marginTop - textPadding);
  
  
  
  // egressId -> bool
  let blockedOrDroppedPackets = {};
  let egressedPackets = {};
  // senderId -> bool
  let senderBlockedOrDroppedPackets = {};
  let sentPackets = {};
  
  for (const [senderId, egressDstProgress] of Object.entries(seqProgress)) {
    for (const [egressId, dstProgress] of Object.entries(egressDstProgress)) {
      const x0 = senderX;
      const y0 = senderYs[senderId];
      const x1 = egressX;
      const y1 = egressYs[egressId];
      
      let a = 255 * min(senderOutFraction(senderId, timeOffsetMillis), egressOutFraction(egressId, timeOffsetMillis));
      
      for (const [dst, progress] of Object.entries(dstProgress)) {
        
        let blocked = false
        let dstBlocked = egressBlocks[egressId]
        if (dstBlocked !== undefined) {
          blocked = (dstBlocked[dst] == true);
        }
        
        let dropped = (egressDrops[egressId] == true);
        
        sentPackets[senderId] = true;
        
        if (blocked || dropped) {
          senderBlockedOrDroppedPackets[senderId] = true;
          blockedOrDroppedPackets[egressId] = true;
          stroke(color(255, 0, 0, a));
        } else {
          egressedPackets[egressId] = true;
          stroke(color(220, 220, 220, a));
        }
        strokeWeight(lerp(1, 4, progress));
        line(x0, y0, x1, y1);
      }
      
      
    }
  }
  
  
  
  
  
  for (const senderId of senderIds) {
    const x = senderX;
    const y = senderYs[senderId];
    
    let a = 255 * senderOutFraction(senderId, timeOffsetMillis);
    
    let hasSentPackets = sentPackets[senderId];
    
    strokeWeight(1);
    stroke(color(0, 0, 0, a));
    if (hasSentPackets) {
      fill(color(220, 220, 220, a));
    } else {
      fill(color(255, 255, 255, a));
    }
    circle(x, y, 2 * r);
    
    noStroke();
    fill(color(0, 0, 0, a));
    textSize(r);
    textAlign(CENTER, CENTER);
    text(senderId, x, y);
    
    let hasBlockedOrDroppedPackets = (senderBlockedOrDroppedPackets[senderId] == true);
    let progress = seqMaxProgress[senderId] || 0;
    
    noStroke();
    if (hasBlockedOrDroppedPackets) {
      fill(color(255, 0, 0, a));
    } else {
      fill(color(0, 0, 0, a));
    }
    textAlign(RIGHT, CENTER);
    text(`${round(100 * progress)}%`, x - r - textPadding, y)
  }
  
  for (const egressId of egressIds) {
    const x = egressX;
    const y = egressYs[egressId];
    
    let a = 255 * egressOutFraction(egressId, timeOffsetMillis);
    
    
    let blockedDsts = [];
    let dstBlocked = egressBlocks[egressId]
    if (dstBlocked !== undefined) {
      blockedDsts = Object.keys(dstBlocked);
      blockedDsts.sort();
    }
    
    let dropped = (egressDrops[egressId] == true);
    
    let hasBlockedOrDroppedPackets = (blockedOrDroppedPackets[egressId] == true);
    let hasEgressedPackets = (egressedPackets[egressId] == true);
    
    strokeWeight(1);
    
    if (hasBlockedOrDroppedPackets) {
      fill(color(255, 0, 0, a));
    } else {
      fill(color(0, 0, 0, a));
    }
    
    noStroke();
    textAlign(LEFT, CENTER);
    if (dropped) {
      text("OFFLINE", x + r + textPadding, y)
    } else if (0 < blockedDsts.length) {
      text(`Partial dst blocked(${blockedDsts.length})`, x + r + textPadding, y)
    }
    
    if (hasBlockedOrDroppedPackets) {
      fill(color(255, 0, 0, a));
    } else if (hasEgressedPackets) {
      fill(color(220, 220, 220, a));
    } else {
      fill(color(255, 255, 255, a));
    }
    if (dropped) {
      stroke(color(255, 0, 0, a));
    } else {
      stroke(color(0, 0, 0, a));
    }
    circle(x, y, 2 * r);
    
    noStroke();
    fill(color(0, 0, 0, a));
    textSize(r);
    textAlign(CENTER, CENTER);
    text(egressId, x, y);
  }
  
  
  

  
  if (packetHeadIndex == packets.length) {
    frameRate(0);
    
    let restartTextSize = 18;
    let restartText = "Tap to restart";
    
    let dx = 0.5 * width;
    let dy = 0.2 * height;
    
    rectMode(CENTER);
    textSize(restartTextSize);
    stroke(color(220, 220, 220));
    fill(color(255, 255, 255));
    rect(dx, dy, textWidth(restartText) + restartTextSize * 2, restartTextSize * 2, 4);
    
    textAlign(CENTER, CENTER);
    noStroke();
    fill(color(0, 0, 0));
    text(restartText, dx, dy); 
  }
}


function senderOutFraction(senderId, timeOffsetMillis) {
  endTimeMillis = senderEndTimeOffsets[senderId];
  if (endTimeMillis !== undefined) {
    const s = endTimeMillis - timeOffsetMillis;
    if (s < 0) {
      return 0;
    } else if (s < transitionOutMillis) {
      return s / transitionOutMillis;
    }
  }
  return 1;
}


function egressOutFraction(egressId, timeOffsetMillis) {
  endTimeMillis = egressEndTimeOffsets[egressId];
  if (endTimeMillis !== undefined) {
    const s = endTimeMillis - timeOffsetMillis;
    if (s < 0) {
      return 0;
    } else if (s < transitionOutMillis) {
      return s / transitionOutMillis;
    }
  }
  return 1;
}


function dstStr(connectionTuple) {
  return `${connectionTuple.dst}:${connectionTuple.dst_port}`
}


function reset() {
  packetHeadIndex = 0;
  eventHeadIndex = 0;
  
  egressIds = [];
  senderIds = [];
  
  egressBlocks = {};
  egressDrops = {};
  
  seqMaxProgress = {};
  
  frameRate(24);
  resetTimeOffsetMillis = millis();
  
  
}

function mousePressed() {
  
  
  reset();
}
