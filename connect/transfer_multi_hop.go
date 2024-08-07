



type TransferPath struct {
	SourceId Id
	DestinationId Id
	StreamId Id
}


// multi hop buffer
// buffers until for the StreamOpen message
// or timeout, then close

// if the buffer closes, it cannot re-open in the future. that would break reliable deliverability
// because each hop by acking the message, is saying as long as it exists it will deliver the messages
// the idle timeout should just be large






