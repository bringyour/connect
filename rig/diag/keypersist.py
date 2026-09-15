#!/usr/bin/env python3
"""PROTOTYPE (sn/miner, not our fork): persist provide secret keys across provider restarts so contracts the platform
signed before a restart still verify. Loads ~/.urnetwork/.provide.keys before provide modes are applied, saves after."""
import sys, pathlib
p = pathlib.Path(sys.argv[1]) / 'run.go'; s = p.read_text()
assert 'readProviderProvideSecretKeys' not in s
old = "\t\tdevice.SetProvideControlMode(sdk.ProvideControlModeAlways)\n"
assert s.count(old) == 1, 'anchor'
new = """\t\t// Provide secret keys sign every contract the platform issues for this
\t\t// provider. Regenerating them on each start fails HMAC verification for any
\t\t// contract signed before the new provide frame commits, which cuts those
\t\t// clients off. Load the previous keys before provide modes are applied.
\t\tif savedProvideSecretKeys, err := readProviderProvideSecretKeys(); err != nil {
\t\t\tfmt.Printf("provider provide secret keys load failed: %s\\n", err)
\t\t} else if savedProvideSecretKeys != nil && 0 < savedProvideSecretKeys.Len() {
\t\t\tdevice.LoadProvideSecretKeys(savedProvideSecretKeys)
\t\t}
""" + old + """\t\tif err := writeProviderProvideSecretKeys(device.GetProvideSecretKeys()); err != nil {
\t\t\tfmt.Printf("provider provide secret keys save failed: %s\\n", err)
\t\t}
"""
s = s.replace(old, new, 1)
helpers = '''
// readProviderProvideSecretKeys loads `~/.urnetwork/.provide.keys`: one
// "<provide mode> <base64 raw key>" line per mode. Returns (nil, nil) when the
// file does not exist.
func readProviderProvideSecretKeys() (*sdk.ProvideSecretKeyList, error) {
	p, err := providerStatePath(".provide.keys")
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	list := sdk.NewProvideSecretKeyList()
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		mode, err := strconv.Atoi(fields[0])
		if err != nil {
			return nil, err
		}
		key, err := base64.StdEncoding.DecodeString(fields[1])
		if err != nil {
			return nil, err
		}
		list.Add(&sdk.ProvideSecretKey{ProvideMode: sdk.ProvideMode(mode), ProvideSecretKey: string(key)})
	}
	return list, nil
}

// writeProviderProvideSecretKeys persists the provide secret keys with 0600
// permissions; anyone holding them can mint contracts this provider accepts.
func writeProviderProvideSecretKeys(list *sdk.ProvideSecretKeyList) error {
	if list == nil || list.Len() == 0 {
		return nil
	}
	p, err := providerStatePath(".provide.keys")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
		return err
	}
	var b strings.Builder
	for i := 0; i < list.Len(); i += 1 {
		k := list.Get(i)
		fmt.Fprintf(&b, "%d %s\\n", int(k.ProvideMode), base64.StdEncoding.EncodeToString([]byte(k.ProvideSecretKey)))
	}
	return os.WriteFile(p, []byte(b.String()), 0600)
}
'''
s = s.rstrip() + '\n' + helpers
for imp in ['"encoding/base64"', '"strconv"', '"strings"', '"errors"', '"path/filepath"', '"os"']:
    if '\t' + imp + '\n' not in s:
        s = s.replace('import (\n', 'import (\n\t' + imp + '\n', 1)
p.write_text(s); print('keypersist patched')
