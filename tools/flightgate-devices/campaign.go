package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// windowRecord is one measurement window of a run, written while the run is
// live (before any log is parsed) so a crash still leaves the throughput.
type windowRecord struct {
	Index         int     `json:"index"`
	StartMillis   int64   `json:"start_millis"`
	EndMillis     int64   `json:"end_millis"`
	Seconds       float64 `json:"seconds"`
	ClientTunRx   int64   `json:"client_tun_rx_bytes"`
	ClientTunTx   int64   `json:"client_tun_tx_bytes"`
	Mbps          float64 `json:"mbps"`
	ClientRadio   string  `json:"client_radio"`
	ProviderRadio string  `json:"provider_radio"`
}

type runMeta struct {
	Tag             string   `json:"tag"`
	ClientRole      string   `json:"client_role"`
	ProviderRole    string   `json:"provider_role"`
	Windows         int      `json:"windows"`
	WindowSeconds   int      `json:"window_seconds"`
	Streams         int      `json:"streams"`
	Url             string   `json:"url"`
	AppVersion      string   `json:"app_version"`
	ConnectCommit   string   `json:"connect_commit"`
	SdkCommit       string   `json:"sdk_commit"`
	ClientProfile   string   `json:"client_profile"`
	ProviderProfile string   `json:"provider_profile"`
	StartMillis     int64    `json:"start_millis"`
	DirectMode      string   `json:"direct_mode"`
	Notes           []string `json:"notes"`
}

func gitShort(dir string) string {
	out, err := exec.Command("git", "-C", dir, "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// tunCounters reads the client's tun interface rx/tx bytes.
func tunCounters(serial string) (string, int64, int64) {
	out, _ := adbShell(serial, "cat /proc/net/dev")
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		name, rest, ok := strings.Cut(line, ":")
		if !ok || !isTunName(name) {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) < 9 {
			continue
		}
		rx, _ := strconv.ParseInt(fields[0], 10, 64)
		tx, _ := strconv.ParseInt(fields[8], 10, 64)
		return name, rx, tx
	}
	return "", 0, 0
}

// isTunName accepts the VPN tunnel (tun0, tun1, ...) and not the kernel's
// ip-in-ip tunl0 device.
func isTunName(name string) bool {
	if !strings.HasPrefix(name, "tun") || len(name) < 4 {
		return false
	}
	for _, c := range name[3:] {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// radio reports "wifi" when Wi-Fi is on and associated, else the cellular
// network type.
func radio(serial string) string {
	wifiOn, _ := adbShell(serial, "settings get global wifi_on")
	ssid, _ := adbShell(serial, "dumpsys wifi 2>/dev/null | grep -m1 'mWifiInfo' | sed 's/.*SSID: \\([^,]*\\),.*/\\1/'")
	network, _ := adbShell(serial, "getprop gsm.network.type")
	if strings.TrimSpace(wifiOn) == "1" && ssid != "" && !strings.Contains(ssid, "unknown") && !strings.Contains(ssid, "<none>") {
		return "wifi"
	}
	return "cell:" + strings.Split(network, ",")[0]
}

func runCampaign(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	client := fs.String("client", "", "client device serial")
	provider := fs.String("provider", "", "provider device serial")
	out := fs.String("out", "", "run directory (created)")
	windows := fs.Int("windows", 12, "measurement windows")
	windowSeconds := fs.Int("window-seconds", 15, "seconds per window")
	streams := fs.Int("streams", 4, "parallel download streams")
	url := fs.String("url", defaultLoadUrl, "download URL")
	tag := fs.String("tag", "", "run tag")
	directMode := fs.String("direct-mode", "stock", "recorded in meta: stock|relay-only|direct-forced")
	if err := fs.Parse(args); err != nil {
		return err
	}
	clientRole, err := role(*client)
	if err != nil {
		return fmt.Errorf("client: %w", err)
	}
	providerRole, err := role(*provider)
	if err != nil {
		return fmt.Errorf("provider: %w", err)
	}
	if *out == "" {
		return errors.New("--out is required")
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		return err
	}
	appVersion, _ := adbShell(*client, "dumpsys package "+appPackage+" | grep -m1 versionName | sed 's/.*=//'")
	meta := runMeta{
		Tag:             *tag,
		ClientRole:      clientRole,
		ProviderRole:    providerRole,
		Windows:         *windows,
		WindowSeconds:   *windowSeconds,
		Streams:         *streams,
		Url:             *url,
		AppVersion:      strings.TrimSpace(appVersion),
		ConnectCommit:   gitShort("../.."),
		SdkCommit:       gitShort("../../../sdk"),
		ClientProfile:   radio(*client),
		ProviderProfile: radio(*provider),
		StartMillis:     time.Now().UnixMilli(),
		DirectMode:      *directMode,
		Notes:           []string{},
	}

	// fresh logcat on both ends, then a full capture each
	for _, serial := range []string{*client, *provider} {
		_, _ = adbShell(serial, "logcat -c")
	}
	captures := []*exec.Cmd{}
	for _, side := range []struct{ serial, name string }{{*client, "client"}, {*provider, "provider"}} {
		file, err := os.Create(filepath.Join(*out, side.name+".logcat"))
		if err != nil {
			return err
		}
		cmd := exec.Command("adb", "-s", side.serial, "logcat", "-v", "epoch")
		cmd.Stdout = file
		cmd.Stderr = file
		if err := cmd.Start(); err != nil {
			return fmt.Errorf("logcat %s: %w", side.name, err)
		}
		captures = append(captures, cmd)
	}
	defer func() {
		for _, cmd := range captures {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()

	tunName, rx0, tx0 := tunCounters(*client)
	if tunName == "" {
		meta.Notes = append(meta.Notes, "client has no tun interface at start; is the tunnel connected?")
	}
	writeJson(filepath.Join(*out, "meta.json"), meta)

	// the workload, on the client, for the whole run plus one window of slack
	loadSeconds := (*windows + 1) * *windowSeconds
	loadFile, err := os.Create(filepath.Join(*out, "load.log"))
	if err != nil {
		return err
	}
	load := exec.Command("adb", "-s", *client, "shell",
		fmt.Sprintf("%s -url %s -streams %d -seconds %d", loadBinary, *url, *streams, loadSeconds))
	load.Stdout = loadFile
	load.Stderr = loadFile
	if err := load.Start(); err != nil {
		return fmt.Errorf("load: %w", err)
	}
	defer func() {
		_, _ = adbShell(*client, "pkill -f flightgate-load")
		_ = load.Wait()
		loadFile.Close()
	}()

	records := []windowRecord{}
	fmt.Printf("run %s: client=%s(%s) provider=%s(%s) tun=%s\n", *tag, clientRole, meta.ClientProfile, providerRole, meta.ProviderProfile, tunName)
	for i := 0; i < *windows; i++ {
		start := time.Now()
		time.Sleep(time.Duration(*windowSeconds) * time.Second)
		_, rx, tx := tunCounters(*client)
		end := time.Now()
		seconds := end.Sub(start).Seconds()
		record := windowRecord{
			Index:         i,
			StartMillis:   start.UnixMilli(),
			EndMillis:     end.UnixMilli(),
			Seconds:       seconds,
			ClientTunRx:   rx - rx0,
			ClientTunTx:   tx - tx0,
			Mbps:          float64(rx-rx0) * 8 / seconds / 1e6,
			ClientRadio:   radio(*client),
			ProviderRadio: radio(*provider),
		}
		rx0, tx0 = rx, tx
		records = append(records, record)
		fmt.Printf("  window %2d: %6.1f Mb/s  client=%s provider=%s\n", i, record.Mbps, record.ClientRadio, record.ProviderRadio)
		writeJson(filepath.Join(*out, "windows.json"), records)
	}
	// give the last diagnostic line a chance to land before the capture stops
	time.Sleep(3 * time.Second)
	return report([]string{*out})
}

func writeJson(path string, value any) {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(path, b, 0o644)
}

// glogRecordStart matches the prefix of a fresh glog record (severity,
// month, day, and a space), which ends any pending continuation.
var glogRecordStart = regexp.MustCompile(`^[IWEF][0-9]{4} `)

// diagSample is the decoded [flightgate] line; counters stay generic maps so
// the tool follows whatever fields the SDK build carries.
type diagSample struct {
	Millis  int64
	Payload map[string]any
}

// parseDiag reads every [flightgate] part line and rejoins the parts that
// share unix_millis into one payload shaped like:
//
//	{state fields..., "p2p": {...}, "provider": {"send_recovery": {...},
//	 "receive": {...}}, "windows": [{"window", "destination",
//	 "send_recovery", "receive"}, ...]}
func parseDiag(path string) ([]diagSample, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	byMillis := map[int64]map[string]any{}
	windowsByMillis := map[int64]map[string]map[string]any{}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024*1024), 8*1024*1024)
	// gomobile's stdout bridge splits one glog record into 1,024-byte logcat
	// entries; a record's continuation is the next GoLog entry that does not
	// itself start a glog record. Join until the JSON parses.
	pending := ""
	for scanner.Scan() {
		line := scanner.Text()
		message := line
		if i := strings.Index(line, "GoLog   : "); i >= 0 {
			message = line[i+len("GoLog   : "):]
		}
		var body string
		if i := strings.Index(message, "[flightgate] "); i >= 0 {
			body = message[i+len("[flightgate] "):]
			pending = ""
		} else if pending != "" && !glogRecordStart.MatchString(message) {
			body = pending + message
		} else {
			continue
		}
		var part map[string]any
		if err := json.Unmarshal([]byte(body), &part); err != nil {
			pending = body
			continue
		}
		pending = ""
		millisValue, _ := part["unix_millis"].(float64)
		millis := int64(millisValue)
		payload := byMillis[millis]
		if payload == nil {
			payload = map[string]any{"windows": []any{}}
			byMillis[millis] = payload
		}
		kind, _ := part["part"].(string)
		switch kind {
		case "state", "memory":
			for k, v := range part {
				if k != "part" {
					payload[k] = v
				}
			}
		case "provider_send", "provider_receive":
			provider, _ := payload["provider"].(map[string]any)
			if provider == nil {
				provider = map[string]any{}
				payload["provider"] = provider
			}
			if kind == "provider_send" {
				provider["send_recovery"] = part["send_recovery"]
			} else {
				provider["receive"] = part["receive"]
			}
		case "window_send", "window_receive":
			destination, _ := part["destination"].(string)
			windows := windowsByMillis[millis]
			if windows == nil {
				windows = map[string]map[string]any{}
				windowsByMillis[millis] = windows
			}
			window := windows[destination]
			if window == nil {
				window = map[string]any{"window": part["window"], "destination": destination}
				windows[destination] = window
			}
			if kind == "window_send" {
				window["send_recovery"] = part["send_recovery"]
			} else {
				window["receive"] = part["receive"]
			}
		}
	}
	samples := []diagSample{}
	for millis, payload := range byMillis {
		windows := []any{}
		for _, window := range windowsByMillis[millis] {
			windows = append(windows, window)
		}
		payload["windows"] = windows
		samples = append(samples, diagSample{Millis: millis, Payload: payload})
	}
	sort.Slice(samples, func(a, b int) bool { return samples[a].Millis < samples[b].Millis })
	return samples, scanner.Err()
}

func num(m map[string]any, keys ...string) float64 {
	var current any = m
	for _, key := range keys {
		next, ok := current.(map[string]any)
		if !ok {
			return 0
		}
		current = next[key]
	}
	switch v := current.(type) {
	case float64:
		return v
	case bool:
		if v {
			return 1
		}
	}
	return 0
}

// sumWindows adds one counter over every window client of a client-side sample.
func sumWindows(sample map[string]any, group string, key string) float64 {
	total := 0.0
	if windows, ok := sample["windows"].([]any); ok {
		for _, w := range windows {
			if m, ok := w.(map[string]any); ok {
				total += num(m, group, key)
			}
		}
	}
	return total
}

// The counters reported per window, as (column, side, group, key). "provider"
// reads the providing device's provider client; "client" sums the client
// device's window clients; "p2p" reads the named device's data-plane counters.
type counterSpec struct {
	column string
	side   string
	group  string
	key    string
}

var counterSpecs = []counterSpec{
	{"prov_flight_wait", "provider", "send_recovery", "UnreliableFlightWaitCount"},
	{"prov_flight_blocked_reliable_cap", "provider", "send_recovery", "UnreliableFlightBlockedWithReliableCapacity"},
	{"prov_flight_gap", "provider", "send_recovery", "UnreliableFlightGapCount"},
	{"prov_flight_gap_reorder", "provider", "send_recovery", "UnreliableFlightGapReorderSuspected"},
	{"prov_flight_timeout", "provider", "send_recovery", "UnreliableFlightTimeoutCount"},
	{"prov_flight_reduction", "provider", "send_recovery", "UnreliableFlightReductionCount"},
	{"prov_timeout_resend", "provider", "send_recovery", "TimeoutResendWriteCount"},
	{"prov_timeout_resend_recent_progress", "provider", "send_recovery", "TimeoutResendWithRecentCumulativeProgress"},
	{"prov_selective_gap_write", "provider", "send_recovery", "SelectiveGapWriteCount"},
	{"prov_ack_write_blocked", "provider", "receive", "AckRouteWriteBlockedCount"},
	{"prov_p2p_fast_send", "p2p-provider", "p2p", "FastSendMessageCount"},
	{"prov_p2p_fast_recv", "p2p-provider", "p2p", "FastReceiveMessageCount"},
	{"prov_p2p_legacy_send", "p2p-provider", "p2p", "LegacySendMessageCount"},
	{"prov_p2p_fast_fallback", "p2p-provider", "p2p", "FastFallbackCount"},
	{"prov_p2p_fast_recv_drop", "p2p-provider", "p2p", "FastReceiveQueueDropCount"},
	{"cli_flight_wait", "client", "send_recovery", "UnreliableFlightWaitCount"},
	{"cli_flight_blocked_reliable_cap", "client", "send_recovery", "UnreliableFlightBlockedWithReliableCapacity"},
	{"cli_flight_gap", "client", "send_recovery", "UnreliableFlightGapCount"},
	{"cli_flight_gap_reorder", "client", "send_recovery", "UnreliableFlightGapReorderSuspected"},
	{"cli_flight_timeout", "client", "send_recovery", "UnreliableFlightTimeoutCount"},
	{"cli_flight_reduction", "client", "send_recovery", "UnreliableFlightReductionCount"},
	{"cli_timeout_resend", "client", "send_recovery", "TimeoutResendWriteCount"},
	{"cli_ack_write_blocked", "client", "receive", "AckRouteWriteBlockedCount"},
	{"cli_p2p_fast_send", "p2p-client", "p2p", "FastSendMessageCount"},
	{"cli_p2p_fast_recv", "p2p-client", "p2p", "FastReceiveMessageCount"},
	{"cli_p2p_legacy_send", "p2p-client", "p2p", "LegacySendMessageCount"},
	{"cli_p2p_fast_recv_drop", "p2p-client", "p2p", "FastReceiveQueueDropCount"},
}

func counterValue(spec counterSpec, clientSample map[string]any, providerSample map[string]any) float64 {
	switch spec.side {
	case "provider":
		if providerSample == nil {
			return 0
		}
		return num(providerSample, "provider", spec.group, spec.key)
	case "client":
		if clientSample == nil {
			return 0
		}
		return sumWindows(clientSample, spec.group, spec.key)
	case "p2p-provider":
		if providerSample == nil {
			return 0
		}
		return num(providerSample, "p2p", spec.key)
	case "p2p-client":
		if clientSample == nil {
			return 0
		}
		return num(clientSample, "p2p", spec.key)
	}
	return 0
}

func baselineSample(samples []diagSample, millis int64) map[string]any {
	if found := lastBefore(samples, millis); found != nil {
		return found
	}
	if len(samples) > 0 {
		return samples[0].Payload
	}
	return nil
}

// lastBefore returns the newest sample at or before millis.
func lastBefore(samples []diagSample, millis int64) map[string]any {
	var found map[string]any
	for _, sample := range samples {
		if sample.Millis > millis {
			break
		}
		found = sample.Payload
	}
	return found
}

type runSummary struct {
	Tag                   string  `json:"tag"`
	Windows               int     `json:"windows"`
	DeadWindows           int     `json:"dead_windows_under_5mbps"`
	MedianMbps            float64 `json:"median_mbps"`
	MinMbps               float64 `json:"min_mbps"`
	MaxMbps               float64 `json:"max_mbps"`
	P2pActive             bool    `json:"p2p_active"`
	P2pFirstWindow        int     `json:"p2p_first_window"`
	DeadWindowsAfterP2p   int     `json:"dead_windows_after_p2p"`
	ClientDiagSamples     int     `json:"client_diag_samples"`
	ProviderDiagSamples   int     `json:"provider_diag_samples"`
	ProviderFlightWait    float64 `json:"provider_flight_wait_total"`
	ProviderBlockedRelCap float64 `json:"provider_flight_blocked_with_reliable_capacity_total"`
	ProviderGapReorder    float64 `json:"provider_gap_reorder_suspected_total"`
	ProviderTimeouts      float64 `json:"provider_flight_timeout_total"`
	ProviderAckBlocked    float64 `json:"provider_ack_write_blocked_total"`
	ProviderFastSend      float64 `json:"provider_p2p_fast_send_total"`
	ClientFastRecv        float64 `json:"client_p2p_fast_recv_total"`
	ClientFastRecvDrops   float64 `json:"client_p2p_fast_recv_drop_total"`
	DirectMode            string  `json:"direct_mode"`
	ProviderPairTypes     string  `json:"provider_selected_pair"`
	ClientPairTypes       string  `json:"client_selected_pair"`
}

// report derives windows.csv and summary.json from a run directory's raw
// files. Counter columns are deltas over the window from the newest
// diagnostic sample at or before each boundary.
func report(args []string) error {
	if len(args) < 1 {
		return errors.New("report needs a run directory")
	}
	dir := args[0]
	var meta runMeta
	if b, err := os.ReadFile(filepath.Join(dir, "meta.json")); err == nil {
		_ = json.Unmarshal(b, &meta)
	}
	var records []windowRecord
	b, err := os.ReadFile(filepath.Join(dir, "windows.json"))
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, &records); err != nil {
		return err
	}
	clientSamples, _ := parseDiag(filepath.Join(dir, "client.logcat"))
	providerSamples, _ := parseDiag(filepath.Join(dir, "provider.logcat"))

	csvFile, err := os.Create(filepath.Join(dir, "windows.csv"))
	if err != nil {
		return err
	}
	defer csvFile.Close()
	writer := csv.NewWriter(csvFile)
	header := []string{"window", "mbps", "client_radio", "provider_radio"}
	for _, spec := range counterSpecs {
		header = append(header, spec.column)
	}
	_ = writer.Write(header)

	summary := runSummary{Tag: meta.Tag, Windows: len(records), P2pFirstWindow: -1,
		ClientDiagSamples: len(clientSamples), ProviderDiagSamples: len(providerSamples),
		DirectMode: meta.DirectMode}
	if n := len(clientSamples); n > 0 {
		if p2p, ok := clientSamples[n-1].Payload["p2p"].(map[string]any); ok {
			summary.ClientPairTypes, _ = p2p["SelectedCandidatePair"].(string)
		}
	}
	if n := len(providerSamples); n > 0 {
		if p2p, ok := providerSamples[n-1].Payload["p2p"].(map[string]any); ok {
			summary.ProviderPairTypes, _ = p2p["SelectedCandidatePair"].(string)
		}
	}
	mbps := []float64{}
	// counters are process-lifetime; the run's baseline is the newest sample
	// before the run started, or the first sample of the capture when the
	// capture began with the run
	previousClient := baselineSample(clientSamples, meta.StartMillis)
	previousProvider := baselineSample(providerSamples, meta.StartMillis)
	for _, record := range records {
		clientSample := lastBefore(clientSamples, record.EndMillis)
		providerSample := lastBefore(providerSamples, record.EndMillis)
		row := []string{strconv.Itoa(record.Index), fmt.Sprintf("%.1f", record.Mbps), record.ClientRadio, record.ProviderRadio}
		p2pInWindow := false
		for _, spec := range counterSpecs {
			delta := counterValue(spec, clientSample, providerSample) - counterValue(spec, previousClient, previousProvider)
			row = append(row, strconv.FormatInt(int64(delta), 10))
			if delta > 0 && (spec.group == "p2p" && strings.Contains(spec.key, "Fast") && strings.Contains(spec.key, "MessageCount")) {
				p2pInWindow = true
			}
			switch spec.column {
			case "prov_flight_wait":
				summary.ProviderFlightWait += delta
			case "prov_flight_blocked_reliable_cap":
				summary.ProviderBlockedRelCap += delta
			case "prov_flight_gap_reorder":
				summary.ProviderGapReorder += delta
			case "prov_flight_timeout":
				summary.ProviderTimeouts += delta
			case "prov_ack_write_blocked":
				summary.ProviderAckBlocked += delta
			case "prov_p2p_fast_send":
				summary.ProviderFastSend += delta
			case "cli_p2p_fast_recv":
				summary.ClientFastRecv += delta
			case "cli_p2p_fast_recv_drop":
				summary.ClientFastRecvDrops += delta
			}
		}
		_ = writer.Write(row)
		mbps = append(mbps, record.Mbps)
		if record.Mbps < 5 {
			summary.DeadWindows++
			if summary.P2pActive {
				summary.DeadWindowsAfterP2p++
			}
		}
		if p2pInWindow && !summary.P2pActive {
			summary.P2pActive = true
			summary.P2pFirstWindow = record.Index
		}
		previousClient, previousProvider = clientSample, providerSample
	}
	writer.Flush()
	if len(mbps) > 0 {
		sorted := append([]float64{}, mbps...)
		sort.Float64s(sorted)
		summary.MedianMbps = sorted[len(sorted)/2]
		summary.MinMbps = sorted[0]
		summary.MaxMbps = sorted[len(sorted)-1]
	}
	writeJson(filepath.Join(dir, "summary.json"), summary)
	fmt.Printf("summary %s: windows=%d dead=%d (after p2p %d) median=%.1f min=%.1f max=%.1f p2p_active=%t first_window=%d diag client/provider=%d/%d\n",
		summary.Tag, summary.Windows, summary.DeadWindows, summary.DeadWindowsAfterP2p, summary.MedianMbps, summary.MinMbps, summary.MaxMbps,
		summary.P2pActive, summary.P2pFirstWindow, summary.ClientDiagSamples, summary.ProviderDiagSamples)
	fmt.Printf("  provider: flight_wait=%.0f blocked_with_reliable_capacity=%.0f gap_reorder=%.0f timeouts=%.0f ack_write_blocked=%.0f fast_send=%.0f\n",
		summary.ProviderFlightWait, summary.ProviderBlockedRelCap, summary.ProviderGapReorder, summary.ProviderTimeouts, summary.ProviderAckBlocked, summary.ProviderFastSend)
	fmt.Printf("  client: fast_recv=%.0f fast_recv_drops=%.0f  mode=%s pairs provider=%q client=%q\n", summary.ClientFastRecv, summary.ClientFastRecvDrops, summary.DirectMode, summary.ProviderPairTypes, summary.ClientPairTypes)
	return nil
}

// runSeries repeats `runs` measurement runs of one role assignment. Every run
// starts from a fresh tunnel (disconnect, settle, reconnect to the peer,
// settle) so the direct-path negotiation is exercised each time, as the
// reporter's per-run provider restart did.
func runSeries(args []string) error {
	fs := flag.NewFlagSet("campaign", flag.ExitOnError)
	client := fs.String("client", "", "client device serial")
	provider := fs.String("provider", "", "provider device serial")
	peerName := fs.String("peer-name", "", "provider's device name substring, as the client sees it")
	out := fs.String("out", "", "series directory (created)")
	runs := fs.Int("runs", 6, "runs")
	windows := fs.Int("windows", 12, "measurement windows per run")
	windowSeconds := fs.Int("window-seconds", 15, "seconds per window")
	streams := fs.Int("streams", 4, "parallel download streams")
	settleSeconds := fs.Int("settle-seconds", 25, "seconds after connect before measuring")
	tag := fs.String("tag", "", "series tag")
	interleaveRelay := fs.Bool("interleave-relay", false, "alternate relay-only (direct mode forced off) and stock runs; --runs counts each kind")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if _, err := role(*client); err != nil {
		return fmt.Errorf("client: %w", err)
	}
	if _, err := role(*provider); err != nil {
		return fmt.Errorf("provider: %w", err)
	}
	if *out == "" || *peerName == "" {
		return errors.New("--out and --peer-name are required")
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		return err
	}
	total := *runs
	if *interleaveRelay {
		total = 2 * *runs
	}
	for i := 0; i < total; i++ {
		runTag := fmt.Sprintf("%s-%02d", *tag, i)
		directMode := "stock"
		if *interleaveRelay {
			if i%2 == 0 {
				directMode = "relay-only"
				runTag += "-relay"
			} else {
				runTag += "-stock"
			}
		}
		fmt.Printf("== %s: fresh tunnel (%s)\n", runTag, directMode)
		if err := disconnect([]string{"--serial", *client}); err != nil {
			fmt.Printf("%s: disconnect: %v\n", runTag, err)
		}
		if *interleaveRelay {
			mode := "clear"
			if directMode == "relay-only" {
				mode = "off"
			}
			if err := allowDirect([]string{"--serial", *client, "--mode", mode}); err != nil {
				return fmt.Errorf("%s: allow-direct: %w", runTag, err)
			}
		}
		time.Sleep(8 * time.Second)
		if err := connectPeer([]string{"--serial", *client, "--name", *peerName}); err != nil {
			return fmt.Errorf("%s: connect: %w", runTag, err)
		}
		time.Sleep(time.Duration(*settleSeconds) * time.Second)
		err := runCampaign([]string{
			"--client", *client, "--provider", *provider,
			"--out", filepath.Join(*out, runTag),
			"--windows", strconv.Itoa(*windows),
			"--window-seconds", strconv.Itoa(*windowSeconds),
			"--streams", strconv.Itoa(*streams),
			"--tag", runTag,
			"--direct-mode", directMode,
		})
		if err != nil {
			fmt.Printf("%s: run: %v\n", runTag, err)
		}
	}
	return nil
}

// seriesReport prints one row per run directory under dir from its
// summary.json, then the series medians.
func seriesReport(args []string) error {
	if len(args) < 1 {
		return errors.New("series-report needs a series directory")
	}
	entries, err := os.ReadDir(args[0])
	if err != nil {
		return err
	}
	fmt.Printf("%-14s %-10s %7s %5s %9s %7s %7s %6s %6s %8s %8s %8s %8s  %s\n", "run", "mode", "median", "dead", "dead>p2p", "min", "max", "p2p", "first", "fl_wait", "blk_cap", "reorder", "ack_blk", "pair(prov/cli)")
	medians := []float64{}
	deadTotal, deadAfter, active := 0, 0, 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(args[0], entry.Name(), "summary.json"))
		if err != nil {
			continue
		}
		var s runSummary
		if err := json.Unmarshal(b, &s); err != nil {
			continue
		}
		fmt.Printf("%-14s %-10s %7.1f %5d %9d %7.1f %7.1f %6t %6d %8.0f %8.0f %8.0f %8.0f  %s/%s\n", entry.Name(), s.DirectMode, s.MedianMbps, s.DeadWindows, s.DeadWindowsAfterP2p, s.MinMbps, s.MaxMbps, s.P2pActive, s.P2pFirstWindow, s.ProviderFlightWait, s.ProviderBlockedRelCap, s.ProviderGapReorder, s.ProviderAckBlocked, s.ProviderPairTypes, s.ClientPairTypes)
		medians = append(medians, s.MedianMbps)
		deadTotal += s.DeadWindows
		deadAfter += s.DeadWindowsAfterP2p
		if s.P2pActive {
			active++
		}
	}
	if len(medians) > 0 {
		sort.Float64s(medians)
		fmt.Printf("series: runs=%d median_of_medians=%.1f dead_windows=%d dead_after_p2p=%d p2p_active_runs=%d\n", len(medians), medians[len(medians)/2], deadTotal, deadAfter, active)
	}
	return nil
}
