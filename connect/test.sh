#!/usr/bin/env bash

for d in `find . -iname '*_test.go' | xargs -n 1 dirname | sort | uniq | paste -sd ' ' -`; do
    if [[ $1 == "" || $1 == `basename $d` ]]; then
        pushd $d
        # highlight source files in this dir
        match="/$(basename $(pwd))/\\S*\.go\|^\\S*_test.go"
        GORACE="log_path=profile/race.out halt_on_error=1" go test -v -race -cpuprofile profile/cpu -memprofile profile/memory -timeout 30m -args -v 0 -logtostderr true | grep --color=always -e "^" -e "$match"
        # -trace profile/trace -coverprofile profile/cover 
        if [[ ${PIPESTATUS[0]} != 0 ]]; then
            exit ${PIPESTATUS[0]}
        fi
        popd
    fi
done
# stdbuf -i0 -o0 -e0 

# to turn on logging e.g.
# go test -args -v 2 -logtostderr true

# go tool trace profile/trace
# PPROF_BINARY_PATH=. go tool pprof profile/cpu

# store default.pgo
# https://go.dev/doc/pgo
