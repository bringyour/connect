// flightgate-devices drives the physical peer-to-peer rig described in
// connect/FLIGHTGATEFIX.md §10 Phase 4: two authorized Android devices, one
// providing to the other as a trusted network peer, a sustained multi-stream
// download through the client, and the SDK's periodic "[flightgate]" JSON
// diagnostic line (sdk/transfer_diag.go) captured from both ends.
//
// Every device action goes through adb. The app side is the debug-only
// FlightGateDebugReceiver (android app, debug source set).
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// The PERFVAR RUN-MAIN allowlist. Public output uses the roles only.
var allowedDevices = map[string]string{
	"3B161FDJG001KT": "device-a",
	"R5CX21FY6ND":    "device-b",
}

const (
	appPackage     = "com.bringyour.network"
	receiver       = appPackage + "/.FlightGateDebugReceiver"
	loginFile      = "/data/local/tmp/flightgate-login"
	loadBinary     = "/data/local/tmp/flightgate-load"
	resultTag      = "FlightGateDebugReceiver"
	defaultLoadUrl = "http://cachefly.cachefly.net/200mb.test"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	command := os.Args[1]
	args := os.Args[2:]
	var err error
	switch command {
	case "preflight":
		err = preflight(args)
	case "profile":
		err = profile(args)
	case "install":
		err = install(args)
	case "load-build":
		err = loadBuild(args)
	case "login":
		err = login(args)
	case "provide":
		err = provide(args)
	case "connect-peer":
		err = connectPeer(args)
	case "disconnect":
		err = disconnect(args)
	case "status":
		err = status(args)
	case "allow-direct":
		err = allowDirect(args)
	case "run":
		err = runCampaign(args)
	case "campaign":
		err = runSeries(args)
	case "report":
		err = report(args)
	case "series-report":
		err = seriesReport(args)
	case "memsteady":
		err = runMemsteady(args)
	case "memsteady-report":
		err = memsteadyReport(args)
	case "memsteady-series":
		err = runMemsteadySeries(args)
	case "build-item":
		err = buildItem(args)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", command, err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: flightgate-devices <preflight|profile|install|load-build|login|provide|connect-peer|disconnect|status|allow-direct|run|campaign|report|series-report|memsteady|memsteady-report|memsteady-series|build-item> [flags]")
}

// role maps a serial to its opaque role, refusing anything off the allowlist.
func role(serial string) (string, error) {
	if r, ok := allowedDevices[serial]; ok {
		return r, nil
	}
	return "", fmt.Errorf("serial is not on the two-device allowlist")
}

func adb(serial string, args ...string) (string, error) {
	full := append([]string{"-s", serial}, args...)
	out, err := exec.Command("adb", full...).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func adbShell(serial string, command string) (string, error) {
	return adb(serial, "shell", command)
}

// preflight lists the attached devices against the allowlist and reports each
// authorized device's battery and network profile. Extra serials are reported
// as a deviation (the PERFVAR block wants none) but do not fail the tool.
func preflight(args []string) error {
	fs := flag.NewFlagSet("preflight", flag.ExitOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	out, err := exec.Command("adb", "devices").CombinedOutput()
	if err != nil {
		return fmt.Errorf("adb devices: %w", err)
	}
	seen := map[string]string{}
	for _, line := range strings.Split(string(out), "\n")[1:] {
		fields := strings.Fields(line)
		if len(fields) >= 2 {
			seen[fields[0]] = fields[1]
		}
	}
	ok := true
	for serial, r := range allowedDevices {
		state, present := seen[serial]
		if !present || state != "device" {
			fmt.Printf("%s: MISSING or not in device state (%q)\n", r, state)
			ok = false
			continue
		}
		fmt.Printf("%s: present\n", r)
		fmt.Printf("  %s\n", deviceProfileLine(serial))
	}
	for serial := range seen {
		if _, allowed := allowedDevices[serial]; !allowed {
			fmt.Printf("deviation: an unlisted serial is attached (left untouched)\n")
		}
	}
	if !ok {
		return errors.New("authorized device set incomplete")
	}
	return nil
}

func deviceProfileLine(serial string) string {
	battery, _ := adbShell(serial, "dumpsys battery 2>/dev/null | grep -m1 level | tr -d ' '")
	wifi, _ := adbShell(serial, "settings get global wifi_on")
	data, _ := adbShell(serial, "settings get global mobile_data")
	radio, _ := adbShell(serial, "getprop gsm.network.type")
	ssid, _ := adbShell(serial, "dumpsys wifi 2>/dev/null | grep -m1 'mWifiInfo' | sed 's/.*SSID: \\([^,]*\\),.*/\\1/'")
	active, _ := adbShell(serial, "dumpsys connectivity 2>/dev/null | grep -m1 'Active default network' ")
	return fmt.Sprintf("battery %s wifi_on=%s ssid=%s mobile_data=%s radio=%s %s",
		battery, wifi, ssid, data, radio, active)
}

// profile switches Wi-Fi and mobile data on one device and waits for the
// requested state to settle.
func profile(args []string) error {
	fs := flag.NewFlagSet("profile", flag.ExitOnError)
	serial := fs.String("serial", "", "device serial")
	wifi := fs.String("wifi", "keep", "on|off|keep")
	data := fs.String("data", "keep", "on|off|keep")
	if err := fs.Parse(args); err != nil {
		return err
	}
	r, err := role(*serial)
	if err != nil {
		return err
	}
	for _, step := range []struct{ svc, want string }{{"wifi", *wifi}, {"data", *data}} {
		switch step.want {
		case "on", "off":
			if _, err := adbShell(*serial, "svc "+step.svc+" "+map[string]string{"on": "enable", "off": "disable"}[step.want]); err != nil {
				return fmt.Errorf("svc %s: %w", step.svc, err)
			}
		case "keep":
		default:
			return fmt.Errorf("bad %s value %q", step.svc, step.want)
		}
	}
	// settle: wait until the default network reflects the request
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(3 * time.Second)
		line := deviceProfileLine(*serial)
		wantWifi := *wifi == "on"
		haveWifi := strings.Contains(line, "wifi_on=1") && !strings.Contains(line, "ssid=<unknown ssid>")
		if *wifi == "keep" || wantWifi == haveWifi {
			fmt.Printf("%s: %s\n", r, line)
			return nil
		}
	}
	return fmt.Errorf("%s: profile did not settle", r)
}

// install replaces the app on the given devices (default: both) with the
// APK. A differently signed build cannot be updated in place, so the existing
// app is uninstalled first; the devices are test devices and this is authorized.
func install(args []string) error {
	fs := flag.NewFlagSet("install", flag.ExitOnError)
	apk := fs.String("apk", "", "path to the debug APK")
	update := fs.Bool("update", false, "update in place (same signature), keeping the app's data and session")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *apk == "" {
		return errors.New("--apk is required")
	}
	serials := fs.Args()
	if len(serials) == 0 {
		for serial := range allowedDevices {
			serials = append(serials, serial)
		}
	}
	for _, serial := range serials {
		r, err := role(serial)
		if err != nil {
			return err
		}
		before, _ := adbShell(serial, "dumpsys package "+appPackage+" | grep -m1 versionName")
		if !*update {
			_, _ = adb(serial, "uninstall", appPackage)
		}
		out, err := adb(serial, "install", "-r", "-g", *apk)
		if err != nil {
			return fmt.Errorf("%s: install: %v: %s", r, err, out)
		}
		after, _ := adbShell(serial, "dumpsys package "+appPackage+" | grep -m1 versionName")
		fmt.Printf("%s: %s -> %s\n", r, strings.TrimSpace(before), strings.TrimSpace(after))
	}
	return nil
}

// loadBuild compiles the on-device load helper for android/arm64 and pushes it
// to both devices, so the client workload is byte-identical on either role.
func loadBuild(args []string) error {
	fs := flag.NewFlagSet("load-build", flag.ExitOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	out := filepath.Join(os.TempDir(), "flightgate-load")
	build := exec.Command("go", "build", "-o", out, "./load")
	build.Env = append(os.Environ(), "GOOS=android", "GOARCH=arm64", "CGO_ENABLED=0")
	if b, err := build.CombinedOutput(); err != nil {
		return fmt.Errorf("build: %v: %s", err, b)
	}
	for serial, r := range allowedDevices {
		if o, err := adb(serial, "push", out, loadBinary); err != nil {
			return fmt.Errorf("%s: push: %v: %s", r, err, o)
		}
		if _, err := adbShell(serial, "chmod 755 "+loadBinary); err != nil {
			return fmt.Errorf("%s: chmod: %w", r, err)
		}
		fmt.Printf("%s: load helper installed\n", r)
	}
	return nil
}

// broadcast sends one receiver action and waits for its FlightGate result
// line, returning that line.
func broadcast(serial string, action string, extras map[string]string, want string, timeout time.Duration) (string, error) {
	start, _ := adbShell(serial, "date +%s")
	cmd := []string{"am", "broadcast", "-a", "com.bringyour.network.debug." + action, "-n", receiver}
	for k, v := range extras {
		cmd = append(cmd, "--es", k, v)
	}
	if _, err := adbShell(serial, strings.Join(cmd, " ")); err != nil {
		return "", fmt.Errorf("broadcast %s: %w", action, err)
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		time.Sleep(time.Second)
		out, _ := adbShell(serial, fmt.Sprintf("logcat -d -v epoch -s %s:I | grep '%s' | tail -1", resultTag, want))
		if out == "" {
			continue
		}
		// only accept a line logged after the broadcast
		if ts := strings.Fields(out); len(ts) > 0 && start != "" {
			if strings.TrimLeft(ts[0], " ") < start {
				continue
			}
		}
		return out, nil
	}
	return "", fmt.Errorf("no %s result within %s", action, timeout)
}

func login(args []string) error {
	fs := flag.NewFlagSet("login", flag.ExitOnError)
	serial := fs.String("serial", "", "device serial")
	userFile := fs.String("user-file", "", "file holding the user auth")
	passFile := fs.String("pass-file", "", "file holding the password")
	if err := fs.Parse(args); err != nil {
		return err
	}
	r, err := role(*serial)
	if err != nil {
		return err
	}
	user, err := os.ReadFile(*userFile)
	if err != nil {
		return err
	}
	pass, err := os.ReadFile(*passFile)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp("", "flightgate-login-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(strings.TrimSpace(string(user)) + "\n" + strings.TrimSpace(string(pass)) + "\n"); err != nil {
		return err
	}
	tmp.Close()
	if o, err := adb(*serial, "push", tmp.Name(), loginFile); err != nil {
		return fmt.Errorf("push: %v: %s", err, o)
	}
	defer adbShell(*serial, "rm -f "+loginFile)
	// the receiver needs a live process: launch the app first
	_, _ = adbShell(*serial, "monkey -p "+appPackage+" -c android.intent.category.LAUNCHER 1 >/dev/null 2>&1")
	time.Sleep(4 * time.Second)
	line, err := broadcast(*serial, "FG_LOGIN", nil, "action=login", 60*time.Second)
	if err != nil {
		return err
	}
	fmt.Printf("%s: %s\n", r, tail(line))
	if !strings.Contains(line, "ok=true") {
		return errors.New("login failed")
	}
	return nil
}

func provide(args []string) error {
	fs := flag.NewFlagSet("provide", flag.ExitOnError)
	serial := fs.String("serial", "", "device serial")
	control := fs.String("control", "", "never|network|always")
	network := fs.String("network", "", "wifi|all")
	if err := fs.Parse(args); err != nil {
		return err
	}
	r, err := role(*serial)
	if err != nil {
		return err
	}
	extras := map[string]string{}
	if *control != "" {
		extras["control"] = *control
	}
	if *network != "" {
		extras["network"] = *network
	}
	line, err := broadcast(*serial, "FG_PROVIDE", extras, "action=provide", 20*time.Second)
	if err != nil {
		return err
	}
	fmt.Printf("%s: %s\n", r, tail(line))
	return nil
}

func connectPeer(args []string) error {
	fs := flag.NewFlagSet("connect-peer", flag.ExitOnError)
	serial := fs.String("serial", "", "device serial")
	name := fs.String("name", "", "peer device name substring")
	if err := fs.Parse(args); err != nil {
		return err
	}
	r, err := role(*serial)
	if err != nil {
		return err
	}
	line, err := broadcast(*serial, "FG_CONNECT_PEER", map[string]string{"name": *name}, "action=connect-peer", 20*time.Second)
	if err != nil {
		return err
	}
	fmt.Printf("%s: %s\n", r, tail(line))
	if strings.Contains(line, "needs_consent=true") {
		fmt.Printf("%s: VPN consent pending; opening the app so it can finish starting the tunnel\n", r)
		_, _ = adbShell(*serial, "monkey -p "+appPackage+" -c android.intent.category.LAUNCHER 1 >/dev/null 2>&1")
	}
	// a run measured without a tunnel is not a run: wait for the tun to appear
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if name, _, _ := tunCounters(*serial); name != "" {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	fmt.Printf("%s: no tun interface within 60 s of connect\n", r)
	return nil
}

func disconnect(args []string) error {
	fs := flag.NewFlagSet("disconnect", flag.ExitOnError)
	serial := fs.String("serial", "", "device serial")
	if err := fs.Parse(args); err != nil {
		return err
	}
	r, err := role(*serial)
	if err != nil {
		return err
	}
	line, err := broadcast(*serial, "FG_DISCONNECT", nil, "action=disconnect", 20*time.Second)
	if err != nil {
		return err
	}
	fmt.Printf("%s: %s\n", r, tail(line))
	return nil
}

func status(args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	serial := fs.String("serial", "", "device serial")
	if err := fs.Parse(args); err != nil {
		return err
	}
	r, err := role(*serial)
	if err != nil {
		return err
	}
	line, err := broadcast(*serial, "FG_STATUS", nil, "status {", 20*time.Second)
	if err != nil {
		return err
	}
	fmt.Printf("%s: %s\n", r, tail(line))
	return nil
}

// tail strips the logcat prefix from a result line.
func tail(line string) string {
	if i := strings.Index(line, resultTag+": "); i >= 0 {
		return line[i+len(resultTag)+2:]
	}
	return line
}

// allowDirect sets the relay-only control on a client for its next connect:
// off forces direct (p2p) mode off, on forces it on, clear restores the
// normal decision.
func allowDirect(args []string) error {
	fs := flag.NewFlagSet("allow-direct", flag.ExitOnError)
	serial := fs.String("serial", "", "device serial")
	mode := fs.String("mode", "clear", "off|on|clear")
	if err := fs.Parse(args); err != nil {
		return err
	}
	r, err := role(*serial)
	if err != nil {
		return err
	}
	line, err := broadcast(*serial, "FG_ALLOW_DIRECT", map[string]string{"mode": *mode}, "action=allow-direct", 20*time.Second)
	if err != nil {
		return err
	}
	fmt.Printf("%s: %s\n", r, tail(line))
	if !strings.Contains(line, "ok=true") {
		return errors.New("allow-direct failed")
	}
	return nil
}
