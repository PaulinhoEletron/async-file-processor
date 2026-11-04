package httpapi

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"strings"

	"async-file-processor/internal/validate"
)

const (
	sampleRows = 200
	// Every sampleBadEvery-th row is invalid, cycling through all the failure kinds.
	sampleBadEvery = 10
)

var (
	sampleNames     = []string{"Alice Smith", "Bruno Costa", "Chloe Martin", "Daniel Kim", "Eva Novak", "Farid Haddad", "Grace Lee", "Hugo Silva"}
	sampleCountries = []string{"US", "BR", "FR", "KR", "CZ", "PT", "DE", "JP"}
)

// getSampleCSV streams a small deterministic CSV in which about one row in ten is invalid, so
// every validation rule shows up in the resulting report.
func (a *api) getSampleCSV(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="sample.csv"`)
	cw := csv.NewWriter(w)
	_ = cw.Write(validate.Header)
	for i := 1; i <= sampleRows; i++ {
		if err := cw.Write(sampleRow(i)); err != nil {
			return
		}
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		loggerFrom(r.Context(), a.Logger).Warn("stream sample csv", "error", err)
	}
}

// sampleRow returns row i (1-based). Row i has id i unless the row is made invalid.
func sampleRow(i int) []string {
	rec := []string{
		fmt.Sprint(i),
		sampleNames[i%len(sampleNames)],
		fmt.Sprintf("user%d@example.com", i),
		fmt.Sprintf("%d.%02d", i*7%500, i*13%100),
		fmt.Sprintf("2024-%02d-%02d", i%12+1, i%28+1),
		sampleCountries[i%len(sampleCountries)],
	}
	if i%sampleBadEvery != 0 {
		return rec
	}
	switch (i / sampleBadEvery) % 10 {
	case 0:
		rec[0] = "abc" // not an integer
	case 1:
		rec[0] = fmt.Sprint(i - 1) // duplicate of the previous row
	case 2:
		rec[1] = "" // required
	case 3:
		rec[1] = strings.Repeat("x", validate.MaxNameLen+1) // too long
	case 4:
		rec[2] = "not-an-email"
	case 5:
		rec[3] = "-12.50" // negative
	case 6:
		rec[3] = "ten" // not a decimal
	case 7:
		rec[4] = "2024-02-30" // not a calendar date
	case 8:
		rec[5] = "usa" // not two uppercase letters
	case 9:
		rec = rec[:5] // wrong field count
	}
	return rec
}
