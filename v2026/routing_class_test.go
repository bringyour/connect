package connect

import "testing"

func TestClassifyOrUnknownNilClassifier(t *testing.T) {
	got := classifyOrUnknown(nil, &IpPath{}, "chrome.exe")
	if got.Class != ClassUnknown {
		t.Fatalf("nil classifier: class=%v want ClassUnknown", got.Class)
	}
	if got.AppId != "chrome.exe" {
		t.Fatalf("nil classifier dropped appId: %q", got.AppId)
	}
}

func TestTrafficClassZeroValueIsUnknown(t *testing.T) {
	var z TrafficClass
	if z != ClassUnknown || z.String() != "unknown" {
		t.Fatalf("zero value must be unknown, got %v/%q", z, z.String())
	}
}
