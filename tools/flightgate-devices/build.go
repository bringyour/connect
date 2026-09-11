package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// buildItem builds the rig's diagnostic Android app from one connect commit:
// a detached connect worktree at that commit, a detached sdk worktree of the
// program's sdk branch beside it (its ../connect replace then resolves to the
// item commit), the sibling module links the builds need, the AAR with the
// transfer diagnostic seam on, and the githubDebug APK. It never touches the
// shared connect worktree stream A works in. Prints the APK path last.
func buildItem(args []string) error {
	fs := flag.NewFlagSet("build-item", flag.ExitOnError)
	commit := fs.String("commit", "", "connect commit to build")
	program := fs.String("program", "/Users/brien/urnetwork/temp/flight-gate-fix", "program directory (connect, sdk, android worktrees)")
	tree := fs.String("tree", "/Users/brien/urnetwork", "the shared checkouts (sibling modules and warp)")
	sdkBranch := fs.String("sdk-branch", "flight-gate-fix", "sdk branch to pair with the commit")
	diagSeconds := fs.String("diag-seconds", "2", "transfer diagnostic interval baked into the AAR")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *commit == "" {
		return errors.New("--commit is required")
	}
	short, err := output("git", "-C", filepath.Join(*program, "connect"), "rev-parse", "--short", *commit)
	if err != nil {
		return fmt.Errorf("resolve commit: %w", err)
	}
	root := filepath.Join(*program, "builds", short)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	connectDir := filepath.Join(root, "connect")
	if _, err := os.Stat(connectDir); err != nil {
		if out, err := output("git", "-C", filepath.Join(*program, "connect"), "worktree", "add", "--detach", connectDir, short); err != nil {
			return fmt.Errorf("connect worktree: %v: %s", err, out)
		}
	}
	sdkDir := filepath.Join(root, "sdk")
	if _, err := os.Stat(sdkDir); err != nil {
		if out, err := output("git", "-C", filepath.Join(*program, "sdk"), "worktree", "add", "--detach", sdkDir, *sdkBranch); err != nil {
			return fmt.Errorf("sdk worktree: %v: %s", err, out)
		}
	}
	for _, sibling := range []string{"glog", "goidenticons", "proxy", "operator-proxy", "userwireguard", "sn", "warp"} {
		link := filepath.Join(root, sibling)
		if _, err := os.Lstat(link); err != nil {
			if err := os.Symlink(filepath.Join(*tree, sibling), link); err != nil {
				return err
			}
		}
	}
	sdkShort, _ := output("git", "-C", sdkDir, "rev-parse", "--short", "HEAD")
	fmt.Printf("build %s: connect %s, sdk %s\n", short, short, sdkShort)

	godebug, _ := output("go", "list", "-f", "{{.DefaultGODEBUG}}", ".")
	if godebug != "" {
		godebug += ",memprofilerate=0"
	} else {
		godebug = "memprofilerate=0"
	}
	ldflag := fmt.Sprintf("-X=runtime.godebugDefault=%s -X github.com/urnetwork/sdk.transferDiagLogSeconds=%s", godebug, *diagSeconds)
	aar := exec.Command("make", "build_android", "MOBILE_RUNTIME_LDFLAG="+ldflag)
	aar.Dir = filepath.Join(sdkDir, "build")
	aar.Env = append(os.Environ(),
		"ANDROID_NDK_HOME=/Users/brien/Library/Android/sdk/ndk/28.0.13004108",
		"WARP_VERSION=flightgate-c"+short+"-s"+sdkShort,
		"URNETWORK_ANDROID_SDK_BUILD_OWNER=flightgate",
	)
	aarLog, err := os.Create(filepath.Join(root, "build-aar.log"))
	if err != nil {
		return err
	}
	aar.Stdout, aar.Stderr = aarLog, aarLog
	err = aar.Run()
	aarLog.Close()
	if err != nil {
		return fmt.Errorf("aar build failed, see %s", filepath.Join(root, "build-aar.log"))
	}
	if _, err := os.Stat(filepath.Join(sdkDir, "build", "android", "URnetworkSdk.aar")); err != nil {
		return errors.New("aar missing after build")
	}

	androidApp := filepath.Join(*program, "android", "app")
	apk := exec.Command("./gradlew", "--no-daemon", ":app:assembleGithubDebug", "-x", "buildSdk")
	apk.Dir = androidApp
	apk.Env = append(os.Environ(), "BRINGYOUR_HOME="+root, "WARP_HOME="+*tree)
	apkLog, err := os.Create(filepath.Join(root, "build-apk.log"))
	if err != nil {
		return err
	}
	apk.Stdout, apk.Stderr = apkLog, apkLog
	err = apk.Run()
	apkLog.Close()
	if err != nil {
		return fmt.Errorf("apk build failed, see %s", filepath.Join(root, "build-apk.log"))
	}
	matches, _ := filepath.Glob(filepath.Join(androidApp, "app", "build", "outputs", "apk", "github", "debug", "*arm64-v8a-debug.apk"))
	if len(matches) == 0 {
		return errors.New("apk missing after build")
	}
	// keep a copy per item so a later build cannot overwrite it
	kept := filepath.Join(root, filepath.Base(matches[0]))
	if data, err := os.ReadFile(matches[0]); err == nil {
		_ = os.WriteFile(kept, data, 0o644)
	}
	fmt.Println(kept)
	return nil
}

func output(name string, args ...string) (string, error) {
	out, err := exec.Command(name, args...).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}
