#!/bin/sh
# Physical peer-to-peer rig for the pinned-provider collapse
# (connect/FLIGHTGATEFIX.md §10 Phase 4). Two authorized Android devices, one
# providing to the other as a network peer, a sustained multi-stream download
# through the client, and the SDK's [flightgate] diagnostic line per window.
#
#   ./run.sh preflight
#   ./run.sh profile --serial S --wifi on|off|keep --data on|off|keep
#   ./run.sh install --apk app-github-debug.apk [--serial S ...]
#   ./run.sh load-build                          build the on-device load helper (android/arm64)
#   ./run.sh login --serial S --user-file F --pass-file F
#   ./run.sh provide --serial S --control never|network|always --network wifi|all
#   ./run.sh connect-peer --serial S --name <device name substring>
#   ./run.sh disconnect --serial S
#   ./run.sh status --serial S
#   ./run.sh run --client S --provider S --out DIR [--windows 12] [--window-seconds 15]
#                [--streams 4] [--url URL] [--tag T]
#   ./run.sh campaign --client S --provider S --peer-name NAME --out DIR [--runs 6] [--tag T]
#                                                repeated runs, each from a fresh tunnel
#   ./run.sh report DIR                          re-derive windows.csv / summary.json from raw logs
#   ./run.sh series-report DIR                   one table over every run directory under DIR
#
# The tool is the Go program in this directory (go run). Credentials are read
# from files and pushed to the device as a file the debug receiver reads; they
# never appear on a command line, in a log, or in a report.
set -eu
cd "$(dirname "$0")"
exec go run . "$@"
