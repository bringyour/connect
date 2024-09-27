package main

import (
	"log"
)

func cluster(coOccurrencePath string) {
	cooc := NewCoOccurrence(nil)
	if err := cooc.LoadData(coOccurrencePath); err != nil {
		log.Fatalf("Error loading co-occurrence data: %v", err)
	}

	// for sid1, sid2Map := range *cooc.cMap {
	// 	for sid2, overlap := range sid2Map {
	// 		log.Printf("%s %s %.9f\n", sid1.String(), sid2.String(), TsFloatv(overlap))
	// 	}
	// }
}
