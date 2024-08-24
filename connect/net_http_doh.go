



package connect

import (
	"strings"

	"golang.org/x/net/idna"
)


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
	

	RequestTimeout time.Duration
	ConnectTimeout time.Duration
	TlsTimeout time.Duration

	TlsConfig *tls.Config
	
}


func DohQuery(ctx context.Context, ipVersion int, domains []string, recordType string, settings *DohSettings) []netip.Addr {
	// run all the queries in parallel to all servers

	switch recordType {
	case "A", "AAAA":
	default:
		return nil, fmt.Errorf("Unsupported record type: %s", recordType)
	}
	
	
	httpClient = &http.Client{
		Timeout: settings.RequestTimeout,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:  settings.ConnectTimeout,
			}).DialContext,
			TLSHandshakeTimeout: settings.TlsTimeout,
			TLSClientConfig: settings.TlsConfig,
		},
	}


	query := func(dohUrl string, domain string)([]netip.Addr) {

		name, err := Punycode(domain)
		if err != nil {
			return nil, err
		}

		params := url.Values{}
		params.Add("name", name)
		params.Add("type", recordType)

		url := fmt.Sprintf("%s?%s", dohUrl, params.Encode())

		request, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, err
		}

		request.Header.Set("Accept", "application/dns-json")
		// note, we do not set the User-Agent for DoH requests
		// see https://bugzilla.mozilla.org/show_bug.cgi?id=1543201#c4

		response, err := httpClient.Do(request)
		if err != nil {
			return nil
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return nil
		}

		data, err := io.ReadAll(rsp.Body)
		if err != nil {
			return nil
		}

		dohResponse := &DohResponse{}
		err = json.Unmarshal(data, dohResponse)
		if err != nil {
			return nil
		}



		if dohResponse.Status != 0 {
			return nil
		}

		ips := []netip.Addr{}
		for _, answer := range dohResponse.Answer {
			if ip, err := netip.ParseAddr(answer.Data); err == nil {
				ips = append(ips, ip)
			}
		}

		return ips
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


	out := make(chan []netip.Addr)

	for _, dohUrl := range dohUrls {
		for _, domain := range domains {
			go func() {
				ips := query(dohUrl, domain)
				select {
				case out <- ips:
				case <- ctx.Done():
				}
			}()
		}
	}

	allIps := map[netip.Addr]bool{}
	for _, dohUrl := range dohUrls {
		for _, domain := range domains {
			select {
			case ips := <- out:
				for _, ip := range ips {
					allIps[ip] = true
				}
			case <- ctx.Done():
			}
		}
	}

	return maps.Keys(allIps)
}


