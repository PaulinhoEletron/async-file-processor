// Package validate checks CSV records against the fixed file schema. It performs no I/O.
package validate

import (
	"fmt"
	"net/mail"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Header lists the required columns in order.
var Header = []string{"id", "name", "email", "amount", "date", "country"}

// MaxNameLen is the maximum length of the name column in characters.
const MaxNameLen = 100

// RowField is the field name reported for errors that concern the whole row.
const RowField = "row"

// FieldError describes one failed rule for one field.
type FieldError struct {
	Field   string
	Message string
}

// ValidateHeader checks that rec is exactly the expected header. Surrounding spaces and a
// leading UTF-8 byte order mark are ignored.
func ValidateHeader(rec []string) error {
	got := make([]string, len(rec))
	for i, c := range rec {
		if i == 0 {
			c = strings.TrimPrefix(c, "\uFEFF")
		}
		got[i] = strings.TrimSpace(c)
	}
	if len(got) != len(Header) {
		return fmt.Errorf("invalid header: expected %d columns (%s), got %d", len(Header), strings.Join(Header, ","), len(got))
	}
	for i, want := range Header {
		if got[i] != want {
			return fmt.Errorf("invalid header: column %d must be %q, got %q (expected %s)", i+1, want, got[i], strings.Join(Header, ","))
		}
	}
	return nil
}

// Validator validates data rows. It remembers the ids it has seen so duplicates can be
// detected, which costs memory proportional to the number of distinct ids. It is not safe for
// concurrent use.
type Validator struct {
	seen map[int64]struct{}
}

// NewValidator returns a Validator with no ids seen.
func NewValidator() *Validator {
	return &Validator{seen: make(map[int64]struct{})}
}

// ValidateRow returns the rule violations of one data row, or nil when the row is valid.
// A row with the wrong number of fields yields a single error. Leading and trailing spaces
// around values are ignored. The row's id is recorded as seen whenever it parses, even if
// other fields are invalid.
func (v *Validator) ValidateRow(rec []string) []FieldError {
	if len(rec) != len(Header) {
		return []FieldError{{RowField, fmt.Sprintf("expected %d fields, got %d", len(Header), len(rec))}}
	}
	var errs []FieldError
	add := func(field, msg string) { errs = append(errs, FieldError{field, msg}) }

	if msg := v.checkID(strings.TrimSpace(rec[0])); msg != "" {
		add("id", msg)
	}
	if msg := checkName(strings.TrimSpace(rec[1])); msg != "" {
		add("name", msg)
	}
	if msg := checkEmail(strings.TrimSpace(rec[2])); msg != "" {
		add("email", msg)
	}
	if msg := checkAmount(strings.TrimSpace(rec[3])); msg != "" {
		add("amount", msg)
	}
	if msg := checkDate(strings.TrimSpace(rec[4])); msg != "" {
		add("date", msg)
	}
	if msg := checkCountry(strings.TrimSpace(rec[5])); msg != "" {
		add("country", msg)
	}
	return errs
}

func (v *Validator) checkID(s string) string {
	if s == "" {
		return "is required"
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return "must be an integer"
	}
	if _, dup := v.seen[n]; dup {
		return "is a duplicate of an earlier row"
	}
	v.seen[n] = struct{}{}
	return ""
}

func checkName(s string) string {
	if s == "" {
		return "is required"
	}
	if utf8.RuneCountInString(s) > MaxNameLen {
		return fmt.Sprintf("must be at most %d characters", MaxNameLen)
	}
	return ""
}

// checkEmail accepts only a bare address: "Name <a@b.c>" parses with net/mail but is rejected.
func checkEmail(s string) string {
	if s == "" {
		return "is required"
	}
	addr, err := mail.ParseAddress(s)
	if err != nil || addr.Address != s {
		return "must be a valid email address"
	}
	return ""
}

// checkAmount accepts plain decimals such as "10", "10.5" or "0.99". Signs, exponents
// ("1e3"), NaN and Inf are rejected; a leading "-" is reported as negative.
func checkAmount(s string) string {
	if s == "" {
		return "is required"
	}
	digits := strings.TrimPrefix(s, "-")
	if !isDecimal(digits) {
		return "must be a decimal number"
	}
	if digits != s {
		return "must be greater than or equal to 0"
	}
	return ""
}

func isDecimal(s string) bool {
	whole, frac, hasPoint := strings.Cut(s, ".")
	return allDigits(whole) && (!hasPoint || allDigits(frac))
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func checkDate(s string) string {
	if s == "" {
		return "is required"
	}
	if _, err := time.Parse("2006-01-02", s); err != nil {
		return "must be a valid date in YYYY-MM-DD format"
	}
	return ""
}

func checkCountry(s string) string {
	if s == "" {
		return "is required"
	}
	if len(s) != 2 || s[0] < 'A' || s[0] > 'Z' || s[1] < 'A' || s[1] > 'Z' {
		return "must be two uppercase letters"
	}
	return ""
}
