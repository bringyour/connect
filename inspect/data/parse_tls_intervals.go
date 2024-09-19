package data

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sort"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"bringyour.com/protocol"
)

const (
	TRANSPORT_DATA_LENGTH_BYTES = 4
	TRANSPORT_TYPE_LENGTH_BYTES = 2
)

type TransportType uint16

const (
	TransportOpen  TransportType = 1
	TransportClose TransportType = 2
	TransportWrite TransportType = 3
	TransportRead  TransportType = 4
)

func (t TransportType) String() string {
	switch t {
	case TransportOpen:
		return "Transport Open"
	case TransportClose:
		return "Transport Close"
	case TransportWrite:
		return "Write Data Chunk"
	case TransportRead:
		return "Read Data Chunk"
	default:
		return "Unknown"
	}
}

// only open is a required field all others are possible to be empty slices or nil
// timestamps are such that open is earliest then reads/writes (ordered separately) then close is latest
// this is guaranteed when the record's fields are populated
type TransportRecord struct {
	Open   *protocol.TransportOpen
	Writes []*protocol.WriteDataChunk // use WriteToBufferEndTime as timestamp
	Reads  []*protocol.ReadDataChunk  // use ReadFromBufferEndTime as timestamp
	Close  *protocol.TransportClose
}

func NewTransportRecord(open *protocol.TransportOpen) *TransportRecord {
	return &TransportRecord{
		Open:   open,
		Writes: make([]*protocol.WriteDataChunk, 0),
		Reads:  make([]*protocol.ReadDataChunk, 0),
		Close:  nil,
	}
}

var byteOrder binary.ByteOrder = binary.BigEndian // default to BigEndian (can be changed to LittleEndian)

func parsePcapFile(pcapFile string, sourceIP string) (map[string]*TransportRecord, error) {
	handle, err := pcap.OpenOffline(pcapFile)
	if err != nil {
		return nil, fmt.Errorf("could not open pcap file: %w", err)
	}
	defer handle.Close()

	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	transportMap := make(map[string]*TransportRecord)

	packets := make([]*gopacket.Packet, 0)

	for packet := range packetSource.Packets() {
		packets = append(packets, &packet)
	}

	// sort packets by timestamp (earliest to latest)
	sort.Slice(packets, func(i, j int) bool {
		return (*packets[i]).Metadata().Timestamp.UnixNano() < (*packets[j]).Metadata().Timestamp.UnixNano()
	})

	for _, packet := range packets {
		ipLayer := (*packet).Layer(layers.LayerTypeIPv4)
		tcpLayer := (*packet).Layer(layers.LayerTypeTCP)

		if ipLayer != nil && tcpLayer != nil {
			ipv4, _ := ipLayer.(*layers.IPv4)
			tcp, _ := tcpLayer.(*layers.TCP)

			// TODO: is there another attribtue we are missing to uniquely identify a connection?
			// like maybe a sequence number?
			key := protocol.TransportKey{
				IpVersion:         4, // IPv4
				TransportProtocol: protocol.IpProtocol_Tcp,
				SourceIp:          ipv4.SrcIP,
				SourcePort:        uint32(tcp.SrcPort),
				DestinationIp:     ipv4.DstIP,
				DestinationPort:   uint32(tcp.DstPort),
			}

			// Determine type of packet (open, read, write, close)
			if tcp.SYN && !tcp.ACK { // connection open (SYN packet)
				if _, exists := transportMap[key.String()]; exists {
					// TODO: should we ignore duplicates like these?
					// i.e., retransmissions of the same SYN packet
					fmt.Println(key.String())
					fmt.Println("Ignoring duplicate open packet")
					continue
				}

				randomInt := rand.Intn(1_000_000)
				var tlsServerName = fmt.Sprintf("%d.placeholder", randomInt) // TODO: add TLS parsing logic later

				transportOpen := &protocol.TransportOpen{
					Key:           &key,
					EgressId:      generateULID(),
					TransportId:   generateULID(),
					OpenTime:      uint64((*packet).Metadata().Timestamp.UnixNano()),
					TlsServerName: &tlsServerName,
				}
				tr := NewTransportRecord(transportOpen)
				transportMap[key.String()] = tr

			} else if tcp.FIN || tcp.RST { // connection close (FIN or RST packet)
				if record, exists := transportMap[key.String()]; exists {
					if record.Close != nil {
						// TODO: there can be multiple closing messages in the current implementation
						// since it can be a fin or rst flag
						// and also we dont check which side of connection sends the termination packet
						// should we?
						// also, should we get the earliest close time or any?
						fmt.Println("Close already exists -> ignoring")
						continue
					}
					transportClose := &protocol.TransportClose{
						TransportId: record.Open.TransportId,
						CloseTime:   uint64((*packet).Metadata().Timestamp.UnixNano()),
					}
					record.Close = transportClose
				} else {
					fmt.Println("Ignoring out of order close packet")
				}
			} else if tcp.PSH && tcp.ACK { // data packet (PSH and ACK set)
				if ipv4.SrcIP.String() == sourceIP { // write packet
					if record, exists := transportMap[key.String()]; exists {
						writeDataChunk := &protocol.WriteDataChunk{
							TransportId: record.Open.TransportId,
							ByteCount:   uint64(len(tcp.Payload)),
							// for now the two times are the same
							WriteToBufferStartTime: uint64((*packet).Metadata().Timestamp.UnixNano()),
							WriteToBufferEndTime:   uint64((*packet).Metadata().Timestamp.UnixNano()),
						}
						record.Writes = append(record.Writes, writeDataChunk)
					} else {
						fmt.Println("Ignoring out of order write packet")
					}
				} else { // read packet
					if record, exists := transportMap[key.String()]; exists {
						readDataChunk := &protocol.ReadDataChunk{
							TransportId: record.Open.TransportId,
							ByteCount:   uint64(len(tcp.Payload)),
							// for now the two times are the same
							ReadFromBufferStartTime: uint64((*packet).Metadata().Timestamp.UnixNano()),
							ReadFromBufferEndTime:   uint64((*packet).Metadata().Timestamp.UnixNano()),
						}
						record.Reads = append(record.Reads, readDataChunk)
					} else {
						fmt.Println("Ignoring out of order read packet")
					}
				}
			}
			// technically currently SYN+ACK from server is ignored and we assume that we are the client in the exchange
		}
	}

	packets = nil

	return transportMap, nil
}

// placeholder for ULID generation
func generateULID() []byte {
	return ulid.Make().Bytes()
}

func saveToFile(data map[string]*TransportRecord, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("could not create file: %w", err)
	}
	defer file.Close()

	for _, transportRecord := range data {
		if transportRecord.Open == nil {
			fmt.Println("Skipping record without open")
			continue
		}

		if err := WriteProtoToFile(file, transportRecord.Open, TransportOpen); err != nil {
			return err
		}

		if transportRecord.Close != nil {
			if err := WriteProtoToFile(file, transportRecord.Close, TransportClose); err != nil {
				return err
			}
		}

		for _, write := range transportRecord.Writes {
			if err := WriteProtoToFile(file, write, TransportWrite); err != nil {
				return err
			}
		}

		for _, read := range transportRecord.Reads {
			if err := WriteProtoToFile(file, read, TransportRead); err != nil {
				return err
			}
		}
	}

	return nil
}

// marshal message then write 4 bytes for length of data, 2 bytes for transport type, followed by the actual data to the file
func WriteProtoToFile(file *os.File, message protoreflect.ProtoMessage, tType TransportType) error {
	data, err := proto.Marshal(message)
	if err != nil {
		return fmt.Errorf("could not marshal %s message: %w", tType.String(), err)
	}

	// TODO: should we check if data size is greater than 2^32 - 1?
	buf := make([]byte, TRANSPORT_DATA_LENGTH_BYTES)
	byteOrder.PutUint32(buf, uint32(len(data)))
	if _, err := file.Write(buf); err != nil {
		return fmt.Errorf("could not write to file: %w", err)
	}

	ttype := make([]byte, TRANSPORT_TYPE_LENGTH_BYTES)
	byteOrder.PutUint16(ttype, uint16(tType))
	if _, err := file.Write(ttype); err != nil {
		return fmt.Errorf("could not write to file: %w", err)
	}

	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("could not write to file: %w", err)
	}

	return nil
}

// assumes that record opens appear before all other items in record
func ReadProtoFromFile(filepath string) (map[ulid.ULID]*TransportRecord, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}

	var offset int64
	transportMap := make(map[ulid.ULID]*TransportRecord, 0)

	for {
		// reading the length of the encoded item before reading each item
		buf := make([]byte, TRANSPORT_DATA_LENGTH_BYTES)
		if _, err := file.ReadAt(buf, offset); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		itemSize := byteOrder.Uint32(buf)
		offset += TRANSPORT_DATA_LENGTH_BYTES

		// reading the transport type
		ttype := make([]byte, TRANSPORT_TYPE_LENGTH_BYTES)
		if _, err := file.ReadAt(ttype, offset); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		transportType := TransportType(byteOrder.Uint16(ttype))
		offset += TRANSPORT_TYPE_LENGTH_BYTES

		// reading the actual encoded item
		item := make([]byte, itemSize)
		if _, err := file.ReadAt(item, offset); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		switch transportType {
		case TransportOpen:
			open := &protocol.TransportOpen{}
			if err := proto.Unmarshal(item, open); err != nil {
				return nil, fmt.Errorf("could not unmarshal %s: %w", transportType.String(), err)
			}
			if record, exists := transportMap[ulid.ULID(open.TransportId)]; exists {
				if record.Open != nil {
					fmt.Println("Duplicate open record")
					continue
				}
				record.Open = open
			} else {
				transportMap[ulid.ULID(open.TransportId)] = NewTransportRecord(open)
			}
		case TransportClose:
			close := &protocol.TransportClose{}
			if err := proto.Unmarshal(item, close); err != nil {
				return nil, fmt.Errorf("could not unmarshal %s: %w", transportType.String(), err)
			}
			if record, exists := transportMap[ulid.ULID(close.TransportId)]; exists {
				if record.Close != nil {
					fmt.Println("Duplicate close record")
					continue
				}
				record.Close = close
			} else {
				fmt.Println("Close record without open")
			}
		case TransportWrite:
			write := &protocol.WriteDataChunk{}
			if err := proto.Unmarshal(item, write); err != nil {
				return nil, fmt.Errorf("could not unmarshal %s: %w", transportType.String(), err)
			}
			if record, exists := transportMap[ulid.ULID(write.TransportId)]; exists {
				record.Writes = append(record.Writes, write)
			} else {
				fmt.Println("Write record without open")
			}
		case TransportRead:
			read := &protocol.ReadDataChunk{}
			if err := proto.Unmarshal(item, read); err != nil {
				return nil, fmt.Errorf("could not unmarshal %s: %w", transportType.String(), err)
			}
			if record, exists := transportMap[ulid.ULID(read.TransportId)]; exists {
				record.Reads = append(record.Reads, read)
			} else {
				fmt.Println("Read record without open")
			}
		default:
			fmt.Println("Unknown transport type")
		}

		offset += int64(itemSize)
	}

	return transportMap, nil
}

func PcapToTransportFiles(dataPath string, savePath string) {
	sourceIP := "172.245.118.233"

	transportRecords, err := parsePcapFile(dataPath, sourceIP)
	if err != nil {
		panic(err)
	}

	if err := saveToFile(transportRecords, savePath); err != nil {
		log.Fatalf("Error saving to binary file: %v", err)
	}

	fmt.Printf("Successfully saved transport records to %q\n", savePath)
}

func LoadTransportsFromFiles(filePath string) (map[ulid.ULID]*TransportRecord, error) {
	return ReadProtoFromFile(filePath)
}

func DisplayTransports(filePath string) error {
	transportMap, err := LoadTransportsFromFiles(filePath)
	if err != nil {
		return err
	}

	for _, record := range transportMap {
		fmt.Println("Open:")
		fmt.Printf("  %+v\n", record.Open)
		if len(record.Writes) > 0 {
			fmt.Println("Writes:")
			for _, write := range record.Writes {
				fmt.Printf("  %+v\n", write)
			}
		}
		if len(record.Reads) > 0 {
			fmt.Println("Reads:")
			for _, read := range record.Reads {
				fmt.Printf("  %+v\n", read)
			}
		}
		if record.Close != nil {
			fmt.Println("Close:")
			fmt.Printf("  %+v\n", record.Close)
		}
		fmt.Println()
	}

	return nil
}
