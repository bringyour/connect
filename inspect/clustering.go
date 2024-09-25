package main

import (
	"log"
)

func cluster(coOccurrencePath string) {
	cooc := NewCoOccurrence(nil)
	if err := cooc.LoadData(coOccurrencePath); err != nil {
		log.Fatalf("Error loading co-occurrence data: %v", err)
	}

	// for tid1, tid2Map := range *cooc.cMap {
	// 	for tid2, overlap := range tid2Map {
	// 		log.Printf("%s %s %.9f\n", tid1.String(), tid2.String(), TsFloatv(overlap))
	// 	}
	// }
}
