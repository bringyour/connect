package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The MEMSTEADY mobile acceptance rules (connect/MEMSTEADY.md, "Scope and
// acceptance signals"), applied to goRuntimeBytes (the SDK sampler's
// go_total_bytes, logged by the transfer diagnostic seam every interval):
// five quiet connected minutes after a burst with p50 and p95 at or below
// 24 MiB, active traffic at or below 24 MiB, no sample above 28 MiB, and
// every temporary client released.
const (
	memsteadyTargetBytes     = 24 * 1024 * 1024
	memsteadyDiagnosticBytes = 28 * 1024 * 1024
)

type memsteadySample struct {
	Millis  int64
	Payload map[string]any
}

type memsteadyPhase struct {
	Samples int     `json:"samples"`
	P50MiB  float64 `json:"p50_mib"`
	P95MiB  float64 `json:"p95_mib"`
	MaxMiB  float64 `json:"max_mib"`
	LastMiB float64 `json:"last_mib"`
	PssP50  float64 `json:"pss_p50_mib"`
	PssMax  float64 `json:"pss_max_mib"`
}

type memsteadyBreach struct {
	Side    string         `json:"side"`
	Phase   string         `json:"phase"`
	Millis  int64          `json:"millis"`
	MiB     float64        `json:"mib"`
	Memory  map[string]any `json:"memory"`
	Windows int            `json:"window_client_count"`
}

type memsteadySide struct {
	Role               string         `json:"role"`
	Burst              memsteadyPhase `json:"burst"`
	Quiet              memsteadyPhase `json:"quiet"`
	WindowClientsBase  int            `json:"window_clients_before_burst"`
	WindowClientsBurst int            `json:"window_clients_burst_max"`
	WindowClientsEnd   int            `json:"window_clients_end"`
	PoolOutstandingEnd int64          `json:"pool_outstanding_end"`
	Pass               bool           `json:"pass"`
	Failures           []string       `json:"failures"`
}

type memsteadySummary struct {
	Tag          string            `json:"tag"`
	Build        string            `json:"build"`
	ClientRole   string            `json:"client_role"`
	ProviderRole string            `json:"provider_role"`
	BurstSeconds int               `json:"burst_seconds"`
	QuietSeconds int               `json:"quiet_seconds"`
	BurstMbps    float64           `json:"burst_mbps"`
	P2pActive    bool              `json:"p2p_active"`
	Client       memsteadySide     `json:"client"`
	Provider     memsteadySide     `json:"provider"`
	Breaches     []memsteadyBreach `json:"breaches"`
	Pass         bool              `json:"pass"`
}

type memsteadyMeta struct {
	Tag          string `json:"tag"`
	Build        string `json:"build"`
	ClientRole   string `json:"client_role"`
	ProviderRole string `json:"provider_role"`
	BurstSeconds int    `json:"burst_seconds"`
	QuietSeconds int    `json:"quiet_seconds"`
	StartMillis  int64  `json:"start_millis"`
	BurstEnd     int64  `json:"burst_end_millis"`
	QuietStart   int64  `json:"quiet_start_millis"`
	EndMillis    int64  `json:"end_millis"`
	TunRxBytes   int64  `json:"burst_tun_rx_bytes"`
	AppVersion   string `json:"app_version"`
}

// pssSample is one whole-app PSS reading (dumpsys meminfo TOTAL PSS), the
// secondary signal 13.7's kernel socket buffers show up in.
type pssSample struct {
	Millis int64  `json:"millis"`
	Side   string `json:"side"`
	PssKiB int64  `json:"pss_kib"`
}

func readPss(serial string) int64 {
	out, _ := adbShell(serial, "dumpsys meminfo "+appPackage+" 2>/dev/null | grep -m1 'TOTAL PSS:' ")
	fields := strings.Fields(out)
	for i, f := range fields {
		if f == "PSS:" && i+1 < len(fields) {
			v, _ := strconv.ParseInt(fields[i+1], 10, 64)
			return v
		}
	}
	return 0
}

// runMemsteady is one MEMSTEADY block on a connected tunnel with the p2p
// lane live: a burst, then quiet connected minutes, capturing the [flightgate]
// memory lines on both devices and whole-app PSS every 15 s.
func runMemsteady(args []string) error {
	fs := flag.NewFlagSet("memsteady", flag.ExitOnError)
	client := fs.String("client", "", "client device serial")
	provider := fs.String("provider", "", "provider device serial")
	out := fs.String("out", "", "run directory (created)")
	tag := fs.String("tag", "", "run tag")
	build := fs.String("build", "", "build label (connect commit)")
	burstSeconds := fs.Int("burst-seconds", 60, "burst length")
	quietSeconds := fs.Int("quiet-seconds", 300, "quiet connected window after the burst")
	streams := fs.Int("streams", 4, "parallel download streams")
	url := fs.String("url", defaultLoadUrl, "download URL")
	heapProfiles := fs.Bool("heap-profile", false, "capture a Go heap profile on both devices at the end of the quiet window")
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
	appVersion, _ := adbShell(*client, "dumpsys package "+appPackage+" 2>/dev/null | grep -m1 versionName | sed 's/.*=//'")
	meta := memsteadyMeta{Tag: *tag, Build: *build, ClientRole: clientRole, ProviderRole: providerRole,
		BurstSeconds: *burstSeconds, QuietSeconds: *quietSeconds, AppVersion: strings.TrimSpace(appVersion)}

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
			return err
		}
		captures = append(captures, cmd)
	}
	defer func() {
		for _, cmd := range captures {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	pss := []pssSample{}
	samplePss := func() {
		now := time.Now().UnixMilli()
		pss = append(pss, pssSample{Millis: now, Side: "client", PssKiB: readPss(*client)})
		pss = append(pss, pssSample{Millis: now, Side: "provider", PssKiB: readPss(*provider)})
		writeJson(filepath.Join(*out, "pss.json"), pss)
	}

	// a settled baseline before the burst
	tunName, rx0, _ := tunCounters(*client)
	fmt.Printf("memsteady %s (%s): client=%s provider=%s tun=%s\n", *tag, *build, clientRole, providerRole, tunName)
	samplePss()
	time.Sleep(20 * time.Second)
	meta.StartMillis = time.Now().UnixMilli()
	writeJson(filepath.Join(*out, "meta.json"), meta)

	loadFile, err := os.Create(filepath.Join(*out, "load.log"))
	if err != nil {
		return err
	}
	load := exec.Command("adb", "-s", *client, "shell",
		fmt.Sprintf("%s -url %s -streams %d -seconds %d", loadBinary, *url, *streams, *burstSeconds))
	load.Stdout = loadFile
	load.Stderr = loadFile
	if err := load.Start(); err != nil {
		return err
	}
	burstStart := time.Now()
	for time.Since(burstStart) < time.Duration(*burstSeconds)*time.Second {
		time.Sleep(15 * time.Second)
		samplePss()
	}
	_ = load.Wait()
	loadFile.Close()
	_, rx1, _ := tunCounters(*client)
	meta.BurstEnd = time.Now().UnixMilli()
	meta.TunRxBytes = rx1 - rx0
	burstMbps := float64(meta.TunRxBytes) * 8 / time.Since(burstStart).Seconds() / 1e6
	fmt.Printf("  burst: %.1f Mb/s over %ds\n", burstMbps, *burstSeconds)
	// the quiet window starts once the burst's ownership has drained
	time.Sleep(10 * time.Second)
	meta.QuietStart = time.Now().UnixMilli()
	writeJson(filepath.Join(*out, "meta.json"), meta)
	quietStart := time.Now()
	for time.Since(quietStart) < time.Duration(*quietSeconds)*time.Second {
		time.Sleep(15 * time.Second)
		samplePss()
	}
	meta.EndMillis = time.Now().UnixMilli()
	writeJson(filepath.Join(*out, "meta.json"), meta)
	if *heapProfiles {
		// last, because the forced collection perturbs the runtime: every
		// acceptance sample above is already recorded
		for _, side := range []struct{ serial, name string }{{*client, "client"}, {*provider, "provider"}} {
			path, err := captureHeapProfile(side.serial, *tag+"-"+side.name, *out)
			if err != nil {
				fmt.Printf("  %s heap profile: %v\n", side.name, err)
				continue
			}
			fmt.Printf("  %s heap profile: %s\n", side.name, path)
		}
	}
	time.Sleep(3 * time.Second)
	return memsteadyReport([]string{*out})
}

func memsteadySamples(path string) ([]memsteadySample, map[int64]int, []diagSample) {
	all, _ := parseDiag(path)
	// parseDiag joins parts by millis; the "memory" part fields land on the
	// payload as part=="memory" entries are merged like state fields
	samples := []memsteadySample{}
	windows := map[int64]int{}
	for _, s := range all {
		if _, ok := s.Payload["go_total_bytes"]; ok {
			samples = append(samples, memsteadySample{Millis: s.Millis, Payload: s.Payload})
		}
		windows[s.Millis] = int(num(s.Payload, "window_client_count"))
	}
	return samples, windows, all
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(math.Ceil(p*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func phaseStats(samples []memsteadySample, from, to int64, pss []pssSample, side string) memsteadyPhase {
	values := []float64{}
	last := 0.0
	for _, s := range samples {
		if s.Millis < from || s.Millis > to {
			continue
		}
		mib := num(s.Payload, "go_total_bytes") / 1048576
		values = append(values, mib)
		last = mib
	}
	sort.Float64s(values)
	phase := memsteadyPhase{Samples: len(values), LastMiB: last}
	if len(values) > 0 {
		phase.P50MiB = percentile(values, 0.5)
		phase.P95MiB = percentile(values, 0.95)
		phase.MaxMiB = values[len(values)-1]
	}
	pssValues := []float64{}
	for _, p := range pss {
		if p.Side == side && p.Millis >= from && p.Millis <= to && p.PssKiB > 0 {
			pssValues = append(pssValues, float64(p.PssKiB)/1024)
		}
	}
	sort.Float64s(pssValues)
	if len(pssValues) > 0 {
		phase.PssP50 = percentile(pssValues, 0.5)
		phase.PssMax = pssValues[len(pssValues)-1]
	}
	return phase
}

// memsteadyReport derives memsteady.json from a run directory and appends a
// row to the MEMSTEADY.md table beside it.
func memsteadyReport(args []string) error {
	if len(args) < 1 {
		return errors.New("memsteady-report needs a run directory")
	}
	dir := args[0]
	var meta memsteadyMeta
	b, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, &meta); err != nil {
		return err
	}
	var pss []pssSample
	if b, err := os.ReadFile(filepath.Join(dir, "pss.json")); err == nil {
		_ = json.Unmarshal(b, &pss)
	}
	summary := memsteadySummary{Tag: meta.Tag, Build: meta.Build, ClientRole: meta.ClientRole, ProviderRole: meta.ProviderRole,
		BurstSeconds: meta.BurstSeconds, QuietSeconds: meta.QuietSeconds, Breaches: []memsteadyBreach{}, Pass: true}
	if meta.BurstEnd > meta.StartMillis {
		summary.BurstMbps = float64(meta.TunRxBytes) * 8 / (float64(meta.BurstEnd-meta.StartMillis) / 1000) / 1e6
	}
	for _, side := range []string{"client", "provider"} {
		samples, windows, all := memsteadySamples(filepath.Join(dir, side+".logcat"))
		s := memsteadySide{Failures: []string{}, Pass: true}
		if side == "client" {
			s.Role = meta.ClientRole
		} else {
			s.Role = meta.ProviderRole
		}
		s.Burst = phaseStats(samples, meta.StartMillis, meta.BurstEnd, pss, side)
		s.Quiet = phaseStats(samples, meta.QuietStart, meta.EndMillis, pss, side)
		for _, sample := range samples {
			mib := num(sample.Payload, "go_total_bytes") / 1048576
			if mib*1048576 > memsteadyDiagnosticBytes {
				phase := "burst"
				if sample.Millis >= meta.QuietStart {
					phase = "quiet"
				} else if sample.Millis < meta.StartMillis {
					phase = "baseline"
				}
				memory := map[string]any{}
				for k, v := range sample.Payload {
					if strings.HasPrefix(k, "go_") || strings.HasPrefix(k, "pool_") || strings.HasPrefix(k, "packet_pool") || k == "goroutines" || k == "physical_bytes" || k == "transport_budget_used_bytes" || k == "gc_cycles" || k == "forced_gc_count" {
						memory[k] = v
					}
				}
				summary.Breaches = append(summary.Breaches, memsteadyBreach{Side: side, Phase: phase, Millis: sample.Millis, MiB: mib, Memory: memory, Windows: windows[sample.Millis]})
			}
		}
		// temporary clients: window client count before the burst, its burst
		// maximum, and at the end of the quiet window
		base, burstMax, end := -1, 0, 0
		for _, a := range all {
			w := windows[a.Millis]
			if a.Millis < meta.StartMillis {
				base = w
			} else if a.Millis <= meta.BurstEnd {
				burstMax = max(burstMax, w)
			}
			if a.Millis <= meta.EndMillis {
				end = w
			}
			if a.Millis <= meta.EndMillis {
				s.PoolOutstandingEnd = int64(num(a.Payload, "pool_outstanding"))
			}
			p2p, _ := a.Payload["p2p"].(map[string]any)
			if p2p != nil && (num(p2p, "FastSendMessageCount") > 0 || num(p2p, "FastReceiveMessageCount") > 0) && a.Millis >= meta.StartMillis {
				summary.P2pActive = true
			}
		}
		s.WindowClientsBase, s.WindowClientsBurst, s.WindowClientsEnd = base, burstMax, end
		if s.Quiet.Samples == 0 {
			s.Failures = append(s.Failures, "no quiet samples")
		}
		if s.Quiet.P50MiB*1048576 > memsteadyTargetBytes || s.Quiet.P95MiB*1048576 > memsteadyTargetBytes {
			s.Failures = append(s.Failures, fmt.Sprintf("quiet p50/p95 %.2f/%.2f MiB above 24 MiB", s.Quiet.P50MiB, s.Quiet.P95MiB))
		}
		// The product ceiling is hard (it is an iOS extension limit), so the
		// worst single sample decides, not only the percentiles.
		if s.Quiet.MaxMiB*1048576 > memsteadyTargetBytes {
			s.Failures = append(s.Failures, fmt.Sprintf("worst quiet sample %.2f MiB above 24 MiB", s.Quiet.MaxMiB))
		}
		if s.Burst.MaxMiB*1048576 > memsteadyTargetBytes {
			s.Failures = append(s.Failures, fmt.Sprintf("active max %.2f MiB above 24 MiB", s.Burst.MaxMiB))
		}
		if base >= 0 && end > base {
			s.Failures = append(s.Failures, fmt.Sprintf("window clients %d at end vs %d before the burst", end, base))
		}
		s.Pass = len(s.Failures) == 0
		if side == "client" {
			summary.Client = s
		} else {
			summary.Provider = s
		}
		summary.Pass = summary.Pass && s.Pass
	}
	if len(summary.Breaches) > 0 {
		summary.Pass = false
	}
	writeJson(filepath.Join(dir, "memsteady.json"), summary)
	headroom := math.Min(24-summary.Client.Quiet.MaxMiB, 24-summary.Provider.Quiet.MaxMiB)
	row := fmt.Sprintf("| %s | %s | %s→%s | %.1f | %.2f / %.2f / %.2f | %.2f / %.2f / %.2f | %.2f / %.2f | %+.2f | %.1f / %.1f | %d | %d→%d / %d→%d | %s |",
		summary.Tag, summary.Build, summary.ClientRole, summary.ProviderRole, summary.BurstMbps,
		summary.Client.Quiet.P50MiB, summary.Client.Quiet.P95MiB, summary.Client.Quiet.MaxMiB,
		summary.Provider.Quiet.P50MiB, summary.Provider.Quiet.P95MiB, summary.Provider.Quiet.MaxMiB,
		summary.Client.Burst.MaxMiB, summary.Provider.Burst.MaxMiB,
		headroom,
		summary.Client.Quiet.PssP50, summary.Provider.Quiet.PssP50,
		len(summary.Breaches),
		summary.Client.WindowClientsBase, summary.Client.WindowClientsEnd, summary.Provider.WindowClientsBase, summary.Provider.WindowClientsEnd,
		map[bool]string{true: "PASS", false: "FAIL: " + strings.Join(append(summary.Client.Failures, summary.Provider.Failures...), "; ")}[summary.Pass])
	table := filepath.Join(filepath.Dir(filepath.Clean(dir)), "MEMSTEADY.md")
	if _, err := os.Stat(table); err != nil {
		header := "# MEMSTEADY device blocks (goRuntimeBytes = go_total_bytes; MiB)\n\n| run | build | roles | burst Mb/s | client quiet p50/p95/max | provider quiet p50/p95/max | active max c/p | worst-case headroom | quiet PSS p50 c/p | >28 MiB | window clients c/p (before→end) | verdict |\n|---|---|---|---|---|---|---|---|---|---|---|---|\n"
		_ = os.WriteFile(table, []byte(header), 0o644)
	}
	f, err := os.OpenFile(table, os.O_APPEND|os.O_WRONLY, 0o644)
	if err == nil {
		_, _ = f.WriteString(row + "\n")
		f.Close()
	}
	fmt.Println(row)
	for _, breach := range summary.Breaches {
		fmt.Printf("  breach %s %s %.2f MiB: %v windows=%d\n", breach.Side, breach.Phase, breach.MiB, breach.Memory, breach.Windows)
	}
	return nil
}

// runMemsteadySeries runs the MEMSTEADY block on a list of builds, each in
// both role assignments (device-b client through device-a providing, then
// the swap), installing each build in place on both devices first. Builds
// are "label=apk-path" pairs, in order.
func runMemsteadySeries(args []string) error {
	fs := flag.NewFlagSet("memsteady-series", flag.ExitOnError)
	deviceA := fs.String("device-a", "3B161FDJG001KT", "device-a serial")
	deviceB := fs.String("device-b", "R5CX21FY6ND", "device-b serial")
	nameA := fs.String("name-a", "Pixel", "device-a's device name substring as a peer")
	nameB := fs.String("name-b", "Samsung", "device-b's device name substring as a peer")
	out := fs.String("out", "", "series directory (created)")
	burstSeconds := fs.Int("burst-seconds", 60, "burst length")
	quietSeconds := fs.Int("quiet-seconds", 300, "quiet connected window")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" || fs.NArg() == 0 {
		return errors.New("--out and at least one label=apk are required")
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		return err
	}
	settle := func(serial string) {
		_, _ = adbShell(serial, "monkey -p "+appPackage+" -c android.intent.category.LAUNCHER 1 >/dev/null 2>&1")
		time.Sleep(8 * time.Second)
	}
	waitPeer := func(serial string, name string) {
		for i := 0; i < 12; i++ {
			line, err := broadcast(serial, "FG_STATUS", nil, "status {", 20*time.Second)
			if err == nil && strings.Contains(line, `"device_name":"`) && strings.Contains(line, `"provide_enabled":true`) && strings.Contains(strings.ToLower(line), strings.ToLower(name)) {
				return
			}
			time.Sleep(10 * time.Second)
		}
	}
	for _, spec := range fs.Args() {
		label, apk, ok := strings.Cut(spec, "=")
		if !ok {
			return fmt.Errorf("bad build spec %q", spec)
		}
		fmt.Printf("=== build %s\n", label)
		if err := install([]string{"--update", "--apk", apk}); err != nil {
			return fmt.Errorf("%s: install: %w", label, err)
		}
		settle(*deviceA)
		settle(*deviceB)
		for _, assignment := range []struct{ tag, client, provider, peerName, providerRole, clientRole string }{
			{"A", *deviceB, *deviceA, *nameA, "device-a", "device-b"},
			{"B", *deviceA, *deviceB, *nameB, "device-b", "device-a"},
		} {
			_ = disconnect([]string{"--serial", assignment.client})
			_ = provide([]string{"--serial", assignment.client, "--control", "never"})
			_ = disconnect([]string{"--serial", assignment.provider})
			_ = provide([]string{"--serial", assignment.provider, "--control", "network", "--network", "all"})
			waitPeer(assignment.client, assignment.peerName)
			if err := connectPeer([]string{"--serial", assignment.client, "--name", assignment.peerName}); err != nil {
				fmt.Printf("%s %s: connect: %v\n", label, assignment.tag, err)
				continue
			}
			time.Sleep(25 * time.Second)
			runTag := label + "-" + assignment.tag
			err := runMemsteady([]string{
				"--client", assignment.client, "--provider", assignment.provider,
				"--out", filepath.Join(*out, runTag), "--tag", runTag, "--build", label,
				"--burst-seconds", strconv.Itoa(*burstSeconds), "--quiet-seconds", strconv.Itoa(*quietSeconds),
			})
			if err != nil {
				fmt.Printf("%s: %v\n", runTag, err)
			}
			_ = disconnect([]string{"--serial", assignment.client})
		}
	}
	return nil
}

// memsteadyAttribute prints, per block and per side, the quiet window's
// percentiles and what the worst sample held. The 24 MiB ceiling is a hard
// product limit and the blocks sit within a fraction of a MiB of it, so the
// question whoever picks this up will ask is which structure holds the bytes.
// The answer this readout gives is that the live heap is only a third of the
// envelope and tracks retained packet-pool ownership, while the rest is Go
// runtime structure no budget constant guards.
//
// Only the fields the periodic sample carries are shown. The split of the
// envelope into heap slack, goroutine stacks and GC metadata comes from the
// runtime's memory classes, which are logged with the heap profile rather
// than every interval, so read those from the "classes=" field of a
// heap-profile line in the same block.
func memsteadyAttribute(args []string) error {
	fs := flag.NewFlagSet("memsteady-attribute", flag.ExitOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() == 0 {
		return errors.New("memsteady-attribute needs one or more block directories")
	}
	fmt.Printf("%-16s %-9s %7s %7s %7s %9s %7s %9s %8s %9s\n",
		"block", "side", "p50", "p95", "worst", "headroom", "live", "poolMiB", "poolN", "goroutines")
	for _, dir := range fs.Args() {
		var meta memsteadyMeta
		b, err := os.ReadFile(filepath.Join(dir, "meta.json"))
		if err != nil {
			continue
		}
		if err := json.Unmarshal(b, &meta); err != nil {
			continue
		}
		for _, side := range []string{"client", "provider"} {
			samples, _, _ := memsteadySamples(filepath.Join(dir, side+".logcat"))
			quiet := []memsteadySample{}
			for _, sample := range samples {
				if meta.QuietStart <= sample.Millis && sample.Millis <= meta.EndMillis {
					quiet = append(quiet, sample)
				}
			}
			if len(quiet) == 0 {
				continue
			}
			values := []float64{}
			var peak memsteadySample
			worst := 0.0
			for _, sample := range quiet {
				mib := num(sample.Payload, "go_total_bytes") / 1048576
				values = append(values, mib)
				if worst < mib {
					worst, peak = mib, sample
				}
			}
			sort.Float64s(values)
			fmt.Printf("%-16s %-9s %7.2f %7.2f %7.2f %+9.2f %7.2f %9.2f %8.0f %9.0f\n",
				filepath.Base(dir), side,
				percentile(values, 0.5), percentile(values, 0.95), worst,
				float64(memsteadyTargetBytes)/1048576-worst,
				num(peak.Payload, "go_live_bytes")/1048576,
				num(peak.Payload, "packet_pool_outstanding_bytes")/1048576,
				num(peak.Payload, "pool_outstanding"),
				num(peak.Payload, "goroutines"))
		}
	}
	return nil
}
