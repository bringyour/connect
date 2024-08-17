package connect

import (
	// "os"
	"flag"
    // "testing"
)


func init() {
	initGlog()
}

func initGlog() {
	flag.Set("logtostderr", "true")
	flag.Set("stderrthreshold", "INFO")
	flag.Set("v", "0")
}
