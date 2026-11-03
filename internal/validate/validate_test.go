package validate

import (
	"reflect"
	"strings"
	"testing"
)

func row(id, name, email, amount, date, country string) []string {
	return []string{id, name, email, amount, date, country}
}

func TestValidateHeader(t *testing.T) {
	good := []string{"id", "name", "email", "amount", "date", "country"}
	for _, tc := range []struct {
		name string
		rec  []string
		ok   bool
	}{
		{"exact", good, true},
		{"spaces", []string{" id", "name ", " email ", "amount", "date", "country"}, true},
		{"bom", []string{"\uFEFFid", "name", "email", "amount", "date", "country"}, true},
		{"wrong order", []string{"name", "id", "email", "amount", "date", "country"}, false},
		{"wrong name", []string{"id", "name", "mail", "amount", "date", "country"}, false},
		{"case differs", []string{"ID", "name", "email", "amount", "date", "country"}, false},
		{"too few", good[:5], false},
		{"too many", append(append([]string{}, good...), "extra"), false},
		{"empty", []string{""}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateHeader(tc.rec); (err == nil) != tc.ok {
				t.Fatalf("ValidateHeader(%q) error = %v, want ok=%v", tc.rec, err, tc.ok)
			}
		})
	}
}

func TestValidateRowFields(t *testing.T) {
	name100 := strings.Repeat("a", 100)
	name101 := strings.Repeat("a", 101)
	runes100 := strings.Repeat("é", 100) // 200 bytes, 100 characters
	for _, tc := range []struct {
		name string
		rec  []string
		want []string // fields reported
	}{
		{"valid", row("1", "Alice", "a@example.com", "10.50", "2024-01-15", "US"), nil},
		{"valid with spaces", row(" 1 ", " Alice ", " a@example.com ", " 10 ", " 2024-01-15 ", " US "), nil},
		{"id missing", row("", "A", "a@example.com", "1", "2024-01-15", "US"), []string{"id"}},
		{"id not integer", row("1.5", "A", "a@example.com", "1", "2024-01-15", "US"), []string{"id"}},
		{"id text", row("abc", "A", "a@example.com", "1", "2024-01-15", "US"), []string{"id"}},
		{"id negative ok", row("-7", "A", "a@example.com", "1", "2024-01-15", "US"), nil},
		{"id overflow", row("99999999999999999999", "A", "a@example.com", "1", "2024-01-15", "US"), []string{"id"}},
		{"name missing", row("1", "", "a@example.com", "1", "2024-01-15", "US"), []string{"name"}},
		{"name blank", row("1", "   ", "a@example.com", "1", "2024-01-15", "US"), []string{"name"}},
		{"name 100 chars", row("1", name100, "a@example.com", "1", "2024-01-15", "US"), nil},
		{"name 101 chars", row("1", name101, "a@example.com", "1", "2024-01-15", "US"), []string{"name"}},
		{"name 100 multibyte", row("1", runes100, "a@example.com", "1", "2024-01-15", "US"), nil},
		{"email missing", row("1", "A", "", "1", "2024-01-15", "US"), []string{"email"}},
		{"email no at", row("1", "A", "not-an-email", "1", "2024-01-15", "US"), []string{"email"}},
		{"email display name", row("1", "A", "Bob <bob@example.com>", "1", "2024-01-15", "US"), []string{"email"}},
		{"email angle only", row("1", "A", "<bob@example.com>", "1", "2024-01-15", "US"), []string{"email"}},
		{"email two addresses", row("1", "A", "a@example.com, b@example.com", "1", "2024-01-15", "US"), []string{"email"}},
		{"amount zero", row("1", "A", "a@example.com", "0", "2024-01-15", "US"), nil},
		{"amount zero decimals", row("1", "A", "a@example.com", "0.00", "2024-01-15", "US"), nil},
		{"amount integer", row("1", "A", "a@example.com", "250", "2024-01-15", "US"), nil},
		{"amount negative", row("1", "A", "a@example.com", "-0.01", "2024-01-15", "US"), []string{"amount"}},
		{"amount exponent", row("1", "A", "a@example.com", "1e3", "2024-01-15", "US"), []string{"amount"}},
		{"amount NaN", row("1", "A", "a@example.com", "NaN", "2024-01-15", "US"), []string{"amount"}},
		{"amount Inf", row("1", "A", "a@example.com", "Inf", "2024-01-15", "US"), []string{"amount"}},
		{"amount plus sign", row("1", "A", "a@example.com", "+5", "2024-01-15", "US"), []string{"amount"}},
		{"amount trailing point", row("1", "A", "a@example.com", "5.", "2024-01-15", "US"), []string{"amount"}},
		{"amount comma", row("1", "A", "a@example.com", "1,5", "2024-01-15", "US"), []string{"amount"}},
		{"amount missing", row("1", "A", "a@example.com", "", "2024-01-15", "US"), []string{"amount"}},
		{"date leap day", row("1", "A", "a@example.com", "1", "2024-02-29", "US"), nil},
		{"date not leap", row("1", "A", "a@example.com", "1", "2023-02-29", "US"), []string{"date"}},
		{"date feb 30", row("1", "A", "a@example.com", "1", "2024-02-30", "US"), []string{"date"}},
		{"date month 13", row("1", "A", "a@example.com", "1", "2024-13-01", "US"), []string{"date"}},
		{"date slashes", row("1", "A", "a@example.com", "1", "2024/01/15", "US"), []string{"date"}},
		{"date no padding", row("1", "A", "a@example.com", "1", "2024-1-5", "US"), []string{"date"}},
		{"date missing", row("1", "A", "a@example.com", "1", "", "US"), []string{"date"}},
		{"country lowercase", row("1", "A", "a@example.com", "1", "2024-01-15", "us"), []string{"country"}},
		{"country long", row("1", "A", "a@example.com", "1", "2024-01-15", "USA"), []string{"country"}},
		{"country short", row("1", "A", "a@example.com", "1", "2024-01-15", "U"), []string{"country"}},
		{"country digits", row("1", "A", "a@example.com", "1", "2024-01-15", "U1"), []string{"country"}},
		{"country non-ascii", row("1", "A", "a@example.com", "1", "2024-01-15", "ÉÉ"), []string{"country"}},
		{"country missing", row("1", "A", "a@example.com", "1", "2024-01-15", ""), []string{"country"}},
		{"several", row("x", "", "bad", "-1", "nope", "xx"), []string{"id", "name", "email", "amount", "date", "country"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			for _, e := range NewValidator().ValidateRow(tc.rec) {
				if e.Message == "" {
					t.Errorf("empty message for field %s", e.Field)
				}
				if e.Rule == "" {
					t.Errorf("empty rule for field %s", e.Field)
				}
				got = append(got, e.Field)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("fields with errors = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestValidateRowWrongWidth(t *testing.T) {
	for _, rec := range [][]string{nil, {"1"}, {"1", "A", "a@example.com", "1", "2024-01-15"}, {"1", "A", "a@example.com", "1", "2024-01-15", "US", "x"}} {
		errs := NewValidator().ValidateRow(rec)
		if len(errs) != 1 || errs[0].Field != RowField {
			t.Fatalf("ValidateRow(%q) = %v, want one %q error", rec, errs, RowField)
		}
	}
}

func TestValidateRowDuplicateID(t *testing.T) {
	v := NewValidator()
	if errs := v.ValidateRow(row("1", "A", "a@example.com", "1", "2024-01-15", "US")); errs != nil {
		t.Fatalf("first row: %v", errs)
	}
	errs := v.ValidateRow(row("1", "B", "b@example.com", "2", "2024-01-16", "BR"))
	if len(errs) != 1 || errs[0].Field != "id" {
		t.Fatalf("duplicate: %v", errs)
	}
	// Padding does not hide a duplicate, and a different id is fine.
	if errs := v.ValidateRow(row(" 1", "B", "b@example.com", "2", "2024-01-16", "BR")); len(errs) != 1 {
		t.Fatalf("padded duplicate: %v", errs)
	}
	if errs := v.ValidateRow(row("2", "B", "b@example.com", "2", "2024-01-16", "BR")); errs != nil {
		t.Fatalf("distinct id: %v", errs)
	}
}

func TestValidateRowClaimsIDEvenWhenRowInvalid(t *testing.T) {
	v := NewValidator()
	if errs := v.ValidateRow(row("5", "", "a@example.com", "1", "2024-01-15", "US")); len(errs) != 1 {
		t.Fatalf("first: %v", errs)
	}
	if errs := v.ValidateRow(row("5", "B", "b@example.com", "1", "2024-01-15", "US")); len(errs) != 1 || errs[0].Field != "id" {
		t.Fatalf("second: %v", errs)
	}
}

func TestValidateRowRuleCodes(t *testing.T) {
	for _, tc := range []struct {
		name string
		rec  []string
		want []FieldError
	}{
		{"required", row("", "", "", "", "", ""), []FieldError{
			{Field: "id", Rule: RuleRequired}, {Field: "name", Rule: RuleRequired}, {Field: "email", Rule: RuleRequired},
			{Field: "amount", Rule: RuleRequired}, {Field: "date", Rule: RuleRequired}, {Field: "country", Rule: RuleRequired}}},
		{"format rules", row("x", strings.Repeat("a", 101), "bad", "abc", "nope", "xx"), []FieldError{
			{Field: "id", Rule: RuleNotInteger}, {Field: "name", Rule: RuleTooLong}, {Field: "email", Rule: RuleInvalidEmail},
			{Field: "amount", Rule: RuleInvalidDecimal}, {Field: "date", Rule: RuleInvalidDate}, {Field: "country", Rule: RuleInvalidCountry}}},
		{"negative", row("1", "A", "a@example.com", "-1", "2024-01-15", "US"), []FieldError{{Field: "amount", Rule: RuleNegative}}},
		{"column count", row("1", "A", "a@example.com", "1", "2024-01-15", "US")[:3], []FieldError{{Field: RowField, Rule: RuleColumnCount}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []FieldError
			for _, e := range NewValidator().ValidateRow(tc.rec) {
				e.Message = ""
				got = append(got, e)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
	v := NewValidator()
	v.ValidateRow(row("1", "A", "a@example.com", "1", "2024-01-15", "US"))
	errs := v.ValidateRow(row("1", "A", "a@example.com", "1", "2024-01-15", "US"))
	if len(errs) != 1 || errs[0].Rule != RuleDuplicate {
		t.Fatalf("duplicate: %+v", errs)
	}
}
