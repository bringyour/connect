package evaluation

type TestCase struct {
	SourceIP         string
	DataPath         string
	SavePath         string
	CoOccurrencePath string
	TimesPath        string
	RegionsFunc      RegionsFunc
}

var TestSession1 = TestCase{
	SourceIP:         "145.94.160.91",
	DataPath:         "data/ts1/ts1.pcapng",
	SavePath:         "data/ts1/ts1_transports.pb",
	CoOccurrencePath: "data/ts1/ts1_cooccurrence.pb",
	TimesPath:        "data/ts1/ts1_times.pb",
	RegionsFunc:      ConstructTestSession1Regions,
}

var TestSession2 = TestCase{
	SourceIP:         "145.94.190.27",
	DataPath:         "data/ts2/ts2.pcapng",
	SavePath:         "data/ts2/ts2_transports.pb",
	CoOccurrencePath: "data/ts2/ts2_cooccurrence.pb",
	TimesPath:        "data/ts2/ts2_times.pb",
	RegionsFunc:      ConstructTestSession2Regions,
}
