# Constant audit — `connect` module

House rule: **tunable values live in settings structs, never package-level
constants.** A behavioral value — a timeout, interval, threshold, capacity,
batch size — belongs on the relevant `*Settings` struct with its default in the
`Default*Settings()` constructor, so embedded callers and tests tune through the
one configuration surface. Wire-format and protocol values are *not* tunables:
both peers (or the platform, or the OS ABI) must agree on them, so they stay
fixed in code.

This audit inventories every package-level `const` in the module (test files
excluded), classifies each, and proposes the migration. Nothing in categories
A–D moves. Category E is the migration backlog, tiered by plumbing cost.

Already migrated (2026-08-08, with the fail-closed encryption work):
`contractWaitLogThreshold` → `SendBufferSettings.ContractWaitLogThreshold`;
the new Required-gate poll interval was introduced directly as
`EncryptionSettings.RequiredCipherPollInterval` (never shipped as a const).

---

## A. Wire / protocol / ABI constants — stay fixed (compatibility-bearing)

Changing any of these breaks interop with peers, the platform, standards, or an
OS ABI. They are facts, not knobs.

| Constant(s) | File | Why fixed |
|---|---|---|
| `DefaultProtocolVersion`, `DefaultStreamVersion`, `MaxMultihopLength` | connect.go | wire protocol / path-array shape |
| `TransportVersion` | transport.go | wire protocol |
| `sequenceTlsKeyLabel`, `sequenceTlsKeyLength`, `sequenceTlsAeadNonceSize`, `sequenceTlsIdentityProofLabel`, `sequenceTlsIdentityProofLength` | transfer_encrypt.go | KDF/AEAD parameters; both peers must agree (DESIGNNOTES §3.3) |
| `VerifyCtx`, `VerifyMsgType*`, `VerifyNonceSize`, `VerifyStatusComplete`, `VerifyMDefault/Min/Max` | verify_wire.go | verify wire protocol |
| `protoWireVarint`, `protoWireBytes` | frame_protobuf.go | protobuf encoding facts |
| `Ipv4HeaderSizeWithoutExtensions`, `Ipv6HeaderSize`, `UdpHeaderSize`, `TcpHeaderSizeWithoutExtensions`, `IcmpHeaderSize`, `icmpUnreachableHeaderSize`, `ipProtocolNumber*`, `tcpFlag*` | ip.go, ip_packet.go, ip_icmp.go | IP-stack facts |
| `icmp4Type*`, `icmp6Type*`, `icmpv4Code*`, `icmpv6Code*` | ip_icmp.go, ip_packet.go | ICMP protocol (+ documented linux errno-delivery reasoning) |
| `icmpErrorIoPending`, `icmpIpSuccess`, `icmp6Reply*Offset`, `ipUnicastIf`, `ipv6UnicastIf` | ip_icmp_egress_windows.go, egress_windows.go | Windows ABI |
| `tlsRecordTypeHandshake`, `tlsVersionMajor`, `tlsHandshakeClientHello`, `tlsExtension*`, `tlsSniTypeHostName` | ip_sni.go | TLS wire format |
| `TlsContentType*`, `TlsVersion1_*` | net_tls.go | RFC values |
| `dtlsVersion*`, `stunMagicCookie`, `quicVersion1/2` | ip_security_webstandard.go | RFC values |
| `dnsTypeSvcb`, `dnsTypeHttps` | ip_mux_upgrade.go | RFC 9460 |
| `maxForwardedDnsResponse` (1232) | ip_mux_upgrade.go | EDNS practical-MTU convention |
| `ReadyHeader` | transport_p2p.go | wire handshake token |
| `MessagePoolMetaByteCount`, `MessagePoolFlag*`, `messagePoolShard*` | message_pool.go | buffer-header/id layout (shard rides the id's low bits) |
| `blockerPepper*`, `blockerMaxHostLen`, `blockerHostRecordLen`, `blockerBlocked*`, `cfaaBlockedPrefix*` | ip_blocker*.go, ip_security_cfaa_block.go | generated blocklist data + record formats (DNS max-host 253 is a fact) |
| `DefaultMtu` (1440) | ip.go | compat-bearing constraint (DESIGNNOTES §7.7); treat as fixed |
| `probeSourcePortMin/Max` (61000–61511) | ip_remote_multi_client_probe.go | anchored to the linux `ip_local_port_range` upper bound; a knob invites collisions — keep, with this note |

## B. Enums, typed states, sentinels — stay (they are types, not tunables)

`peekResult`, `ProviderState`, `probeClass`, `WindowType`,
`multiClientEventType`, `warnCause`, `donorVerdict`, `blackholeReason`,
`verdictActionKind`, `cfaaVerdict`, `dmcaVerdict`, `SecurityPolicyResult` (+
`securityPolicyStatsUnknownResult` sentinel), `IpProtocol`,
`dialFailureAction`, `EncryptionMode`, `sequenceTlsRole`,
`ExtenderConnectMode`, `PacketTranslationMode`, `TransportControl`,
`TransportMode` (+ `modePreferenceNone` sentinel), `PeerType`,
`peerConnectionAdmissionReason`, `peerConnectionTeardownStage`,
`HttpUpgradeMode`, `relPrefix`/`relExitIdLength` (log format identity).
`TransportMaxPriority/MinPriority/MaxWeight/MinWeight` define the public
priority/weight scale — API contract, keep.

## C. Compile-time debug flags — stay (deliberately not runtime settings)

`debugTags`, `debugVerifyHeaders`, `DebugCloseSend`,
`DebugTransferCopyOnWrite` (var). Dev-only switches; making them settings
would put dead branches on hot paths in production configs.

## D. Defaults that back existing settings fields — acceptable pattern

These are `default*` constants whose only job is to seed a `Default*Settings()`
field (the documented pattern). Optional cleanup: inline the literal into the
constructor like most settings do, so the default lives in exactly one place.

`defaultTransferBufferSize` (transfer.go), `defaultIpBufferSize`,
`defaultUdpFlowBufferSize`, `defaultTcpFlowBufferSize`,
`defaultUdpReceiveShardCount` (ip.go), `defaultIcmpFlowBufferSize`
(ip_icmp.go), `defaultMaxPairPartnersPerActivity`,
`defaultMaxComponentNodeCount` (ip_assoc.go), `defaultMaxInflightDnsQueries`,
`defaultReverseMaxEntries` (ip_mux_upgrade.go), `defaultProbeTimeout`
(ip_remote_multi_client_probe.go), `defaultHeartbeatInterval`
(ip_remote_multi_client_observability.go), `DefaultDnsUpgradeMaskAddress`
(net_http_doh.go — exported deployment default).

## E. Tunables to migrate into settings — the backlog

### Tier 1 — mechanical (a settings struct already reaches the use site)

| Constant | File | Proposed home |
|---|---|---|
| `sendPackBatchMaxFrames`, `sendPackBatchMaxMessageByteCount` | transfer.go | `SendBufferSettings` (coalescer shape; also aliased by `providerReturnBatchMaxFrames/Bytes` in ip.go) |
| `receiveDeliverBatchMaxFrames` | transfer.go | `ReceiveBufferSettings` |
| `maxRejectedStreamOpens` | transfer_stream_manager.go | stream-manager settings |
| `backendDegradedFailThreshold`, `backendDegradedWindow` | transport.go | platform-transport settings |
| `platformWebSocketWriteBatchMaxMessages` | transport.go | platform-transport settings |
| `webSocketWriteBatchMaxByteCount` | net_websocket_batch.go | platform-transport settings |
| `clientTlsSessionCacheCapacity`, `tlsClientSessionCacheCapacity` | net_tls.go | connect/net settings (also: two near-duplicate names — unify) |
| `reconnectFastPathLimit`, `reconnectFastPathMaxDelay` | net_http.go | client-strategy settings |
| `dohServerWeightFloor`, `maxDohResponseBytes`, `dohStaleServeBound`, `dohQueryReserveByteCount`, `dnsLookupReserveByteCount`, `dohTlsSessionCacheCapacity`, `dohSeedMaxScore`, `dohWarmDomain` | net_http_doh.go | DoH settings |
| `firstLoadMaxFlows`, `firstLoadMaxDnsQueries`, `firstLoadWindow`, `firstLoadFlowExpiration`, `firstLoadLogQueueSize` | net_first_load.go | first-load observability settings |
| `providerUdpIdleTimeout`, `providerUdpFlowByteCount`, `providerTcpFlowByteCount`, `providerIcmpFlowByteCount`, `providerIcmpTargetDivisor`, `providerMin*Limit` | ip.go | egress/provider settings |
| `providerP2pPriorityRefreshInterval`, `lruEvictionSampleSize` | ip.go | egress/provider settings |
| `icmpTransactionTimeoutMillis`, `icmpReplyBufferSlack` | ip_icmp_egress_windows.go | ICMP engine settings |
| `uplinkStampCoarseness`, `uplinkStalenessMaxHold`, `collapseDeadlineLifetimes`, `standingReserveSpares`, `flowOwnerCacheMaxCount`, `pinnedFollowWindowMultiple`, `schedulerPauseProbeInterval`, `inferredDialFailureTimeout`, `dialProbeMaxSends`, `quarantineMemoryDuration`, `busyProbeUnsendableConvictions`, `dialStrikeWindow`, `dialStarvedFailureThreshold`, `dialStarvedMinDestinations`, `comparativeReceivingSiblings`, `maxRestoredWindowIdentityCount` | ip_remote_multi_client*.go | `MultiClientSettings` |
| `probeSendTimeout`, `probeDefaultQueryName`, `probePassFraction` | ip_remote_multi_client_probe.go, ip_probe_targets.go | probe settings |
| `QualificationMaxAge`, `qualificationMaxEntries` | ip_remote_multi_client_probe.go | probe settings (`QualificationMaxAge` is exported — keep an exported accessor or migrate callers) |
| `proberScanInterval`, `proberConcurrency`, `proberReprobeInterval`, `proberAttemptMinInterval` | ip_remote_multi_client_prober.go | prober settings |
| `relLineMaxChars` | ip_remote_multi_client_observability.go | observability settings |
| `sniMaxClientHelloBytes`, `sniMaxFlows`, `sniFlowTtl` | ip_sni.go | SNI settings |
| `maxDnsRespondersPerQuestion`, `maxServerNamesPerIp`, `reverseEvictSampleSize`, `tunnelDohColdFailureCount`, `tunnelDohWarmLease`, `dnsColdProbeInitialInterval`, `dnsColdProbeMaxInterval`, `muxDohHttpWaveSize`, `muxFallbackDohHttpWaveSize`, `muxDohWarmServerStagger`, `maxDnsTcpConnections`, `maxDnsTcpFlows`, `maxDnsTcpQueryBytes`, `dnsTcpFlowTtl`, `dnsTcpIoTimeout` | ip_mux_upgrade.go | `DnsUpgradeSettings` |
| `syntheticSpeedDefaultByteCount`, `syntheticSpeedMaxByteCount`, `syntheticSpeedChunkByteCount` | ip_synthetic_speed.go | synthetic-speed settings |
| `securityPolicyStatsMaxDestinationsPerResult` | ip_security.go | security settings |
| `blockActionEvictSampleSize` | ip_block_action.go | blocker settings |
| `ipAssocPruneScanMin`, `ipAssocScratchShrinkMin` | ip_assoc.go | `IpAssocSettings` |
| `recoveryTrackerMaxAge`, `recoveryTrackerMaxEntries` | reliability_metrics.go | reliability-metrics settings |
| `peerConnectionFactoryRetryTimeout`, `peerConnectionPriorityTimeout`, `maxPeerConnectionPriorityCount`, `maxRememberedNetworkPeerCount`, `peerConnectionSlowTeardownTimeout`, `maxBufferedRemoteIceCandidateCount/Bytes`, `maxIceCandidatesPerSignalFrame`, `maxIceCandidateBytesPerSignalFrame` | transport_p2p_webrtc.go | `WebRtcSettings` / `P2pTransportSettings` |

### Tier 2 — needs construction rework (package-level pools / shard counts fixed at init)

These size global pools or shard arrays created at package init or type
construction; moving them means threading a settings value into that
construction (or explicitly accepting them as structural):

`decodedPackOwnerPoolShards/PoolCapacity/InlineFrames` (frame_protobuf.go),
`decodedTransferFramePoolCapacity` (frame_protobuf.go), `packetPoolSize`,
`packetPoolFloorCount`, `largeObjectPoolFloorByteCount` (message_pool.go),
`rawSendPackPoolCapacity`, `sendItemPoolCapacity`,
`rejectedReceiveSequenceCapacity` (transfer.go), `dmcaFlowShards`
(ip_security_dmca.go), `tunTcpInboundBurstPacketCount`,
`tunTcpInboundShardCount` (tun.go).

### Tier 3 — deliberately argued constants — decision needed, recommend keep

- `clientLifetimeJitterMinFraction` (ip_remote_multi_client.go): its comment
  explicitly argues "a constant rather than a setting: the exact fraction is
  not worth a knob." Keeping honors that recorded decision; migrating for
  uniformity contradicts it. **Recommend: keep, cite the comment.**
- `maxIpAssocEntityCount` (ip_assoc.go): hard bound of the dense uint16 index —
  structural, not tunable. **Keep.**

---

## Suggested execution

1. **Tier 1** in one sweep per settings home (multi-client, mux/DoH, webrtc,
   transport, ip/egress, misc), each hunk = add fields + defaults + replace
   reads; behavior-neutral by construction (defaults equal today's constants).
   Respect the settings-copy rule (constructors never mutate caller settings).
2. **Tier 2** case by case — pools may be better served by the existing
   `MemoryScaledByteCount` pattern than by raw knobs.
3. **Tier 3** left as recorded decisions unless overridden.
