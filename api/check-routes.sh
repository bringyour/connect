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

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# --- the route table -------------------------------------------------------
# Every NewRoute( occurrence, not every line: some source lines hold two.
route_files=()
for f in $server_dir/api/api.go $server_dir/oauth/handlers.go; do
    [[ -f $f ]] && route_files+=$f
done

grep -oh 'NewRoute("[A-Z*]*", "[^"]*"' $route_files \
    | sed -e 's/.*NewRoute("//' -e 's/", "/ /' -e 's/"$//' \
    | sed -e 's/\\\\\./\./g' -e 's/(\[^\/\]+)/*/g' \
    | sort -u > $work/routes

# --- the spec --------------------------------------------------------------
# Paths are the two-space keys under `paths:`; methods are their four-space
# children. Stops at the next top-level key (components:).
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
' $spec | sed -e 's/{[^}]*}/*/g' | sort -u > $work/spec

missing_from_spec=$(comm -23 $work/routes $work/spec)
missing_from_server=$(comm -13 $work/routes $work/spec)

route_count=$(grep -c . $work/routes)
spec_count=$(grep -c . $work/spec)

drifted=0

if [[ -n $missing_from_spec ]]; then
    print -u2 "registered but NOT in the spec:"
    print -u2 -- "$missing_from_spec" | sed 's/^/  /' >&2
    drifted=1
fi

if [[ -n $missing_from_server ]]; then
    print -u2 "in the spec but NOT registered:"
    print -u2 -- "$missing_from_server" | sed 's/^/  /' >&2
    drifted=1
fi

if (( drifted == 0 )); then
    print "ok: $route_count registered routes, $spec_count documented operations, no drift"
else
    print -u2 "drift: $route_count registered routes, $spec_count documented operations"
fi

exit $drifted
