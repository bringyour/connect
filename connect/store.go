package connect

import (
	
	"bringyour.com/protocol"
)

// a responsive document that the connect lib and client is built on
// coordinates many connect clients on a single document

// protobuf protocol
// store documents
// status: pending, committed


// be able to have read only mounts in the document from other public/system sources
// 


// IMPORTANT change_id stored per path
// IMPORTANT change id propagates down to root


// stitches are applied in order and keyed to the change_id for the path
// when change_id changes, stitches are removed
// stitch subscribe status is PENDING (versus COMMITTED)
// general pattern:
// store.Stitch(path, filterFn)
// client.SendControl(AControMessage)



// client.Subscribe(path, callback)(unsubFn)

// client.Filter(path, filterFn)
// new node commands are diffed with old
// command diffs are sent to server with the change_id of path
// if server sees the same change id of path, commands are applied


// list ops are a linked list, next
// when store client generates an insert or remove from a list, the link entries are also updated separate ops (ADD, LINK)