package grouping

import (
	"testing"
)

func TestSplitDomain(t *testing.T) {
	tests := []struct {
		domain              string
		expectedSubdomain   string
		expectedThirdLevel  string
		expectedSecondLevel string
		expectedTopLevel    string
	}{
		{
			domain:              "streaming123.something.netflix.com",
			expectedSubdomain:   "streaming123",
			expectedThirdLevel:  "something.netflix.com",
			expectedSecondLevel: "netflix.com",
			expectedTopLevel:    ".com",
		},
		{
			domain:              "something.netflix.com",
			expectedSubdomain:   "",
			expectedThirdLevel:  "something.netflix.com",
			expectedSecondLevel: "netflix.com",
			expectedTopLevel:    ".com",
		},
		{
			domain:              "netflix.com",
			expectedSubdomain:   "",
			expectedThirdLevel:  "",
			expectedSecondLevel: "netflix.com",
			expectedTopLevel:    ".com",
		},
		{
			domain:              "a.b.c.d.example.co.uk",
			expectedSubdomain:   "a.b.c.d",
			expectedThirdLevel:  "example.co.uk",
			expectedSecondLevel: "co.uk",
			expectedTopLevel:    ".uk",
		},
		{
			domain:              "localhost",
			expectedSubdomain:   "",
			expectedThirdLevel:  "",
			expectedSecondLevel: "localhost",
			expectedTopLevel:    "",
		},
	}

	for _, test := range tests {
		t.Run(test.domain, func(t *testing.T) {
			subDomain, thirdLevel, secondLevel, topLevel := splitDomain(test.domain)

			if subDomain != test.expectedSubdomain {
				t.Errorf("expected subdomain %q, got %q", test.expectedSubdomain, subDomain)
			}

			if thirdLevel != test.expectedThirdLevel {
				t.Errorf("expected thirdLevel %q, got %q", test.expectedThirdLevel, thirdLevel)
			}

			if secondLevel != test.expectedSecondLevel {
				t.Errorf("expected secondLevel %q, got %q", test.expectedSecondLevel, secondLevel)
			}

			if topLevel != test.expectedTopLevel {
				t.Errorf("expected topLevel %q, got %q", test.expectedTopLevel, topLevel)
			}
		})
	}
}
