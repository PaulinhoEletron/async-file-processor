// Command gencsv writes a CSV in the processor's schema with a chosen fraction of invalid rows.
//
//	go run ./scripts/gencsv -rows 1000000 -bad 0.05 -out big.csv
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"strings"
)

// kinds are the ways a row can be made invalid. Each produces exactly one invalid row.
const (
	badEmail = iota
	badName
	badAmount
	badDate
	badCountry
	badWidth
	badDuplicate
	badKinds
)

func main() {
	rows := flag.Int("rows", 1_000_000, "number of data rows")
	bad := flag.Float64("bad", 0.05, "fraction of rows that are invalid (0 to 1)")
	out := flag.String("out", "", "output file (default stdout)")
	seed := flag.Uint64("seed", 1, "random seed; the same seed gives the same file")
	flag.Parse()
	if *rows < 0 || *bad < 0 || *bad > 1 {
		fmt.Fprintln(os.Stderr, "gencsv: -rows must be >= 0 and -bad between 0 and 1")
		os.Exit(2)
	}

	var w io.Writer = os.Stdout
	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			fmt.Fprintln(os.Stderr, "gencsv:", err)
			os.Exit(1)
		}
		defer f.Close()
		w = f
	}
	bw := bufio.NewWriterSize(w, 1<<20)

	badRows, err := generate(bw, *rows, *bad, *seed)
	if err == nil {
		err = bw.Flush()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "gencsv:", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "rows=%d bad=%d valid=%d\n", *rows, badRows, *rows-badRows)
}

// generate streams rows to w and returns how many of them are invalid.
func generate(w *bufio.Writer, rows int, badFraction float64, seed uint64) (int, error) {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	countries := []string{"US", "BR", "FR", "DE", "JP", "KR", "GB", "CA", "PT", "ES"}
	if _, err := w.WriteString("id,name,email,amount,date,country\n"); err != nil {
		return 0, err
	}
	badRows := 0
	claimed := 0 // most recent row whose id appears in the file
	var sb strings.Builder
	for i := 1; i <= rows; i++ {
		id := fmt.Sprint(i)
		name := fmt.Sprintf("Customer %d", i)
		email := fmt.Sprintf("user%d@example.com", i)
		amount := fmt.Sprintf("%d.%02d", rng.IntN(100000), rng.IntN(100))
		date := fmt.Sprintf("%04d-%02d-%02d", 2020+rng.IntN(6), 1+rng.IntN(12), 1+rng.IntN(28))
		country := countries[rng.IntN(len(countries))]
		extra := ""
		replacedID := false

		if rng.Float64() < badFraction {
			badRows++
			switch rng.IntN(badKinds) {
			case badEmail:
				email = "user" + id + "-at-example.com"
			case badName:
				if rng.IntN(2) == 0 {
					name = ""
				} else {
					name = strings.Repeat("x", 101)
				}
			case badAmount:
				amount = []string{"-5.00", "abc", "1e3", ""}[rng.IntN(4)]
			case badDate:
				date = []string{"2024-02-30", "2024-13-01", "15/01/2024", ""}[rng.IntN(4)]
			case badCountry:
				country = []string{"usa", "U", "b2", ""}[rng.IntN(4)]
			case badWidth:
				extra = ",unexpected"
			case badDuplicate:
				// Reuse the id of an earlier row that kept its own. Row 1 has none, so break
				// its email instead.
				if claimed == 0 {
					email = "user" + id + "-at-example.com"
				} else {
					id = fmt.Sprint(claimed)
					replacedID = true
				}
			}
		}
		if !replacedID && extra == "" { // rows of the wrong width are rejected before their id is read
			claimed = i
		}
		sb.Reset()
		sb.WriteString(id)
		sb.WriteByte(',')
		sb.WriteString(name)
		sb.WriteByte(',')
		sb.WriteString(email)
		sb.WriteByte(',')
		sb.WriteString(amount)
		sb.WriteByte(',')
		sb.WriteString(date)
		sb.WriteByte(',')
		sb.WriteString(country)
		sb.WriteString(extra)
		sb.WriteByte('\n')
		if _, err := w.WriteString(sb.String()); err != nil {
			return 0, err
		}
	}
	return badRows, nil
}
