package util

import "testing"

func TestTimestamp(t *testing.T) {
	var validExamples = []string{
		"2002-05-13T14:02:30.123Z",
		"2009-05-13T14:02:30.123",
		"2023-12-12T05:02:30Z",
		"2024-01-01T14:02:59",
		"2002-05-13T14:02",
		"2002-05-13",
		"123456789",
	}

	for _, example := range validExamples {
		if val, err := ParseTimestamp(example); err != nil {
			t.Fatalf("Error converting %s - err %v", example, err)
		} else {
			t.Logf("converted %s to: %v", example, val)
		}
	}

	var invalidExample = "12345A237634"
	if val, err := ParseTimestamp(invalidExample); err != nil {
		t.Logf("Invalid timestamp %s rejected - err %v", invalidExample, err)
	} else {
		t.Fatalf("Invalid timestamp %s not rejected and converted to: %v", invalidExample, val)
	}
}

func TestStringArraysEqual(t *testing.T) {
	var reference = []string{"a", "b", "c"}
	var differences = [][]string{
		{"c", "a", "b"},
		{"a", "a", "a"},
		{"a"},
	}

	for _, different := range differences {
		if StringArraysEqual(reference, different) {
			t.Fatalf("Only arrays with the same elements in the same order are considered equal. %s : %s", reference, different)
		}
	}

	var equal = []string{"a", "b", "c"}
	if !StringArraysEqual(reference, equal) {
		t.Fatalf("Arrays with the same elements in the same order shall be considered equal. %s : %s", reference, equal)
	}
}
