package run

import (
	"strings"
	"testing"
)

func TestGCSJoin(t *testing.T) {
	testCases := []struct {
		input []string
		want  string
	}{
		{[]string{"gs://foo/bar", "/baz"}, "gs://foo/bar/baz"},
		{[]string{"gs://foo/bar", "baz"}, "gs://foo/bar/baz"},
		{[]string{"gs://foo/bar/", "baz"}, "gs://foo/bar/baz"},
		{[]string{"gs://foo/bar/", "/baz"}, "gs://foo/bar/baz"},
		{[]string{"/foo/", "gs://bar", "gs://baz"}, "/foo/bar/baz"},
		{[]string{}, ""},
	}
	for _, tc := range testCases {
		t.Run(strings.Join(tc.input, ","), func(t *testing.T) {
			if got, want := gcsJoin(tc.input...), tc.want; got != want {
				t.Fatalf("Unexpected result: got %q, want %q", got, want)
			}
		})
	}
}
