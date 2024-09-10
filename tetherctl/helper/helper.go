package helper

import (
	"io"
	"net/http"
	"strings"

	"bringyour.com/wireguard/logger"
)

// getLogLevel returns the log level from the log string.
//
// Log levels include silent(s), verbose(v)/debug(d) and error(e).
// Default log level is error.
func GetLogLevel(log string) int {
	logL := strings.ToLower(log)
	switch logL {
	case "verbose", "debug", "v", "d":
		return logger.LogLevelVerbose
	case "silent", "s":
		return logger.LogLevelSilent
	default:
		return logger.LogLevelError
	}
}

// get ip{v4,v6} of this machine
func GetPublicIP(isIPv4 bool) (string, error) {
	request := "https://api.ipify.org?format=text"
	if !isIPv4 {
		request = "https://api6.ipify.org?format=text"
	}
	resp, err := http.Get(request)
	if err != nil {
		return "", err
	}

	defer resp.Body.Close()

	ip, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(ip), nil
}
