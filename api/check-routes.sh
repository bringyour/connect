#!/usr/bin/env zsh
#
# Pin bringyour.yml to the server's route table.
#
#   ./check-routes.sh /path/to/urnetwork/server
#
# Extracts every router.NewRoute("METHOD", "/path", ...) registration the api
# service serves (api/api.go plus the oauth authorization server routes it
# appends) and every method+path the spec documents, normalizes the two into
# the same shape, and exits non-zero if either side has something the other
# does not.
#
# The /competition/* routes are the one exception. This api process serves
# them, but they are a separate security domain with their own document,
# sn/api/competition.yml, so bringyour.yml must NOT list them: a competition
# path found there is drift. They are checked against that document instead,
# looked up at <server>/../sn/api/competition.yml; when it is not reachable
# that half of the check is skipped with a note rather than failed, so the
# script still works in a checkout without the sn repo beside the server.
#
# Deliberately dependency-free: zsh, grep, sed, sort, comm. No yaml parser and
# no go toolchain, so it runs in a bare checkout and in CI.
#
# Normalization:
#   route  ([^/]+)   -> *      spec  {name} -> *
#   route  \.        -> .      (the route table escapes dots for its regex)
#
# That second rule matters for the three /.well-known/ discovery documents,
# which are the only routes registered with escaped dots. The server's own
# api/spec_conformance_test.go normalizePath() does NOT unescape them, so it
# reads those three spec paths as unimplemented; one `strings.ReplaceAll(p,
# "\\.", ".")` there lines the two checks up.
#
# Test files are excluded: they register routes that are not served.

set -u
emulate -L zsh
setopt no_nomatch pipe_fail

script_dir=${0:A:h}
spec=$script_dir/bringyour.yml

if [[ $# -ne 1 ]]; then
    print -u2 "usage: ${0:t} <path to the urnetwork server checkout>"
    exit 2
fi

server_dir=${~1}
if [[ ! -d $server_dir ]]; then
    print -u2 "${0:t}: not a directory: $server_dir"
    exit 2
fi
if [[ ! -f $server_dir/api/api.go ]]; then
    print -u2 "${0:t}: $server_dir does not look like the server checkout (no api/api.go)"
    exit 2
fi
if [[ ! -f $spec ]]; then
    print -u2 "${0:t}: spec not found: $spec"
    exit 2
fi

competition_spec=${server_dir:A:h}/sn/api/competition.yml

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# spec_paths <file> -- the "METHOD /path" operations an OpenAPI document
# declares. Paths are the two-space keys under `paths:`; methods are their
# four-space children. Stops at the next top-level key (components:).
spec_paths() {
    awk '
        /^paths:[[:space:]]*$/ { inpaths = 1; next }
        /^[A-Za-z]/            { inpaths = 0 }
        !inpaths               { next }
        /^  \/[^ ]*:[[:space:]]*$/ { path = $1; sub(/:$/, "", path); next }
        /^    (get|put|post|patch|delete|head|options|trace):[[:space:]]*$/ {
            method = $1
            sub(/:$/, "", method)
            print toupper(method), path
        }
    ' "$1" | sed -e 's/{[^}]*}/*/g' | sort -u
}

# --- the route table -------------------------------------------------------
# Every NewRoute( occurrence, not every line: some source lines hold two.
route_files=()
for f in $server_dir/api/api.go $server_dir/oauth/handlers.go; do
    [[ -f $f ]] && route_files+=$f
done

grep -oh 'NewRoute("[A-Z*]*", "[^"]*"' $route_files \
    | sed -e 's/.*NewRoute("//' -e 's/", "/ /' -e 's/"$//' \
    | sed -e 's/\\\\\./\./g' -e 's/(\[^\/\]+)/*/g' \
    | sort -u > $work/routes.all

grep -v ' /competition/' $work/routes.all > $work/routes
grep ' /competition/' $work/routes.all > $work/routes.competition

# --- the specs -------------------------------------------------------------
spec_paths $spec > $work/spec.all
grep -v ' /competition/' $work/spec.all > $work/spec
grep ' /competition/' $work/spec.all > $work/spec.competition

drifted=0

report() {
    print -u2 "$1:"
    print -u2 -- "$2" | sed 's/^/  /' >&2
    drifted=1
}

# --- bringyour.yml ---------------------------------------------------------
missing_from_spec=$(comm -23 $work/routes $work/spec)
missing_from_server=$(comm -13 $work/routes $work/spec)

[[ -n $missing_from_spec ]] && report "registered but NOT in bringyour.yml" "$missing_from_spec"
[[ -n $missing_from_server ]] && report "in bringyour.yml but NOT registered" "$missing_from_server"

# a competition path in bringyour.yml is drift on its own: they belong to
# sn/api/competition.yml and must not be dual-listed
listed_competition=$(cat $work/spec.competition)
[[ -n $listed_competition ]] && \
    report "in bringyour.yml but belongs in sn/api/competition.yml" "$listed_competition"

# --- sn/api/competition.yml ------------------------------------------------
route_count=$(grep -c . $work/routes)
spec_count=$(grep -c . $work/spec)
competition_route_count=$(grep -c . $work/routes.competition)

if [[ -f $competition_spec ]]; then
    spec_paths $competition_spec > $work/spec.sn
    competition_spec_count=$(grep -c . $work/spec.sn)

    competition_missing=$(comm -23 $work/routes.competition $work/spec.sn)
    competition_extra=$(comm -13 $work/routes.competition $work/spec.sn)

    [[ -n $competition_missing ]] && \
        report "registered but NOT in sn/api/competition.yml" "$competition_missing"
    [[ -n $competition_extra ]] && \
        report "in sn/api/competition.yml but NOT registered" "$competition_extra"

    competition_summary="$competition_route_count competition routes, $competition_spec_count documented in sn/api/competition.yml"
else
    competition_summary="$competition_route_count competition routes NOT CHECKED (no $competition_spec)"
fi

if (( drifted == 0 )); then
    print "ok: $route_count registered routes, $spec_count documented operations; $competition_summary; no drift"
else
    print -u2 "drift: $route_count registered routes, $spec_count documented operations; $competition_summary"
fi

exit $drifted
