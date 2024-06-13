package connect



// FIXME scramble block to store records in a larger file until full, e.g. 1TB scramble block
// FIXME the only use case is enumerating the entire scramble block


/*
func CreateProviderAudit(record *protocol.EgressRecord) *protocol.ProviderAudit {

}

func CreateEncryptedAccountRecord(partial *AccountRecordPartial, clientAccountId, providerAccountId) []byte {

}


func MatchesAuditRecords(key *EgressKey, record *EncryptedEgressRecord) *protocol.EncryptEgressRecord {

}


// this is a brute force search to decrypt the record
func DecryptAuditRecords(key *EgressKey, encryptedEgressRecord []byte, encryptedAccountRecord []byte) (
	*EgressRecord,
	*AccountRecord,
	error
) {

}


type AuditRecord struct {
	egress *EgressRecord
	account *AccountRecord
}


func ScanAuditRecords(ctx context.Context, paths []string, egressKeys EgressKeyEnumeration, parallelCount int) *AuditRecord {

}


func AppendAuditRecord(ctx context.Context, path string, encryptedEgressRecord *EncryptedEgressRecord, encryptedAccountRecord []byte) {

}



func EgressKeyTimeRange(startTime time.Time, duration time.Duration, protocol, sourceIp, sourcePort, destinationIp, destinationPort) EgressKeyEnumeration {

}
*/


