#!/usr/bin/env bash
# Bulk-remove leaked socks5 test clients. Safe: only device_spec=socks5 with no connections.
# Auth: a network JWT, either exported as JWT or read from JWT_FILE on the provider host ($PROVIDER_HOST).
# The platform API base is https://api.$PLATFORM_DOMAIN (see env.example).
JWT="${JWT:-$(ssh -o BatchMode=yes "root@${PROVIDER_HOST:?set PROVIDER_HOST}" "cat ${JWT_FILE:?set JWT_FILE}" 2>/dev/null)}"
API="https://api.${PLATFORM_DOMAIN:?set PLATFORM_DOMAIN}"
curl -s -H "Authorization: Bearer $JWT" "$API/network/clients" -o /tmp/_cl.json 2>/dev/null
python3 - "$JWT" "$API" <<'PY'
import json,sys,urllib.request
jwt,api=sys.argv[1],sys.argv[2]
try: cs=json.load(open('/tmp/_cl.json')).get('clients') or []
except Exception: sys.exit(0)
mine=[x["client_id"] for x in cs if x.get('device_spec')=='socks5' and not x.get('connections')]
if not mine: sys.exit(0)
body=json.dumps({"client_ids":mine}).encode()
req=urllib.request.Request(f"{api}/network/remove-clients",data=body,
    headers={"Authorization":f"Bearer {jwt}","Content-Type":"application/json"},method="POST")
try:
    urllib.request.urlopen(req,timeout=120).read()
    print(f"  [cleanup] removed {len(mine)} leaked clients")
except Exception as e:
    print(f"  [cleanup] failed: {e}")
PY
