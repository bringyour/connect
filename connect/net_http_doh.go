



package connect

import (
	"strings"

	"golang.org/x/net/idna"
)


// based on https://github.com/likexian/doh



type DohQuestion struct {
	Name string `json:"name"`
	Type int    `json:"type"`
}

type DohAnswer struct {
	Name string `json:"name"`
	Type int    `json:"type"`
	TTL  int    `json:"TTL"`
	Data string `json:"data"`
}

type DohResponse struct {
	Status   int        `json:"Status"`
	TC       bool       `json:"TC"`
	RD       bool       `json:"RD"`
	RA       bool       `json:"RA"`
	AD       bool       `json:"AD"`
	CD       bool       `json:"CD"`
	Question []DohQuestion `json:"Question"`
	Answer   []DohAnswer   `json:"Answer"`
	Provider string     `json:"provider"`
}


func Punycode(domain string) (string, error) {
	name := strings.TrimSpace(domain)

	return idna.New(
		idna.MapForLookup(),
		idna.Transitional(true),
		idna.StrictDomainName(false),
	).ToASCII(name)
}


// https://1.1.1.1/dns-query
// https://9.9.9.9:5053/dns-query



// https://pkg.go.dev/golang.org/x/net@v0.4.0/nettest#SupportsIPv6
// SupportsIPv4
// SupportsIPv6


// https://developers.cloudflare.com/1.1.1.1/ip-addresses/
// https://www.quad9.net/

dohUrlsIpv4 := []string{
	"https://1.1.1.1/dns-query",
	"https://1.0.0.1/dns-query",
	"https://9.9.9.9:5053/dns-query",
	"https://149.112.112.112:5053/dns-query",
}

dohUrlsIpv6 := []string{
	"https://[2606:4700:4700::1111]/dns-query",
	"https://[2606:4700:4700::1001]/dns-query",
	"https://[2620:fe::fe]:5053/dns-query",
	"https://[2620:fe::9]:5053/dns-query",
}


type DohSettings {
	TlsConfig *tls.Config

	HttpTimeout time.Duration
	HttpConnectTimeout time.Duration
	HttpTlsTimeout time.Duration
	
}


// runs all queries in parallel
// domain -> doh -> ips
func DohQuery(ctx context.Context, ipVersion int, domains []string, recordType string, settings *DohSettings) (map[string]map[string][]net.IP, error) {

	switch recordType {
	case "A", "AAAA":
	default:
		return nil, fmt.Errorf("Unsupported record type: %s", recordType)
	}
	

	// FIXME
	// run all the queries in parallel to all servers

	httpClient = &http.Client{
		Timeout: settings.HttpTimeout,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:  settings.HttpConnectTimeout,
			}).DialContext,
			TLSHandshakeTimeout: settings.HttpTlsTimeout,
			TLSClientConfig: tlsConfig,
		},
	}


	query := func(dohUrl string, domain string) {

		name, err := Punycode(domain)
		if err != nil {
			return nil, err
		}

		param := url.Values{}
		param.Add("name", name)
		param.Add("type", strings.TrimSpace(string(t)))

		dnsURL := fmt.Sprintf("%s?%s", dohUrl, param.Encode())

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, dnsURL, nil)
		if err != nil {
			return nil, err
		}

		req.Header.Set("Accept", "application/dns-json")
		// note, we do not set the User-Agent for DoH requests
		// see https://bugzilla.mozilla.org/show_bug.cgi?id=1543201#c4

		rsp, err := httpClient.Do(req)
		if err != nil {
			return nil, err
		}

		defer rsp.Body.Close()
		if rsp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("bad status code: %d", rsp.StatusCode)
		}

		data, err := io.ReadAll(rsp.Body)
		if err != nil {
			return nil, err
		}

		rr := &dns.Response{
			Provider: c.String(),
		}

		err = json.Unmarshal(data, rr)
		if err != nil {
			return nil, err
		}

		if rr.Status != 0 {
			return rr, fmt.Errorf("bad response code: %d", rr.Status)
		}

		return rr, nil
	}


	var dohUrls []string
	switch ipVersion {
	case 4:
		dohUrls = dohUrlsIpv4
	case 6:
		dohUrls = dohUrlsIpv6
	default:
		dohUrls = []string{}
	}


	for _, dohUrl := range dohUrls {
		for _, domain := range domains {
			// FIXME collecting results
			go query(dohUrl, domain)		
		}
	}



}


