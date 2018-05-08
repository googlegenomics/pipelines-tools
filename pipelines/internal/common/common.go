// Package common provides functions used by multiple commands.
package common

import (
	"flag"
	"fmt"
	"path"
	"strings"

	"github.com/google/go-genproto/googleapis/rpc/code"
	genomics "google.golang.org/api/genomics/v2alpha1"
)

// ExpandOperationName adds the project and operations prefixes to name (if
// they are not already present).
func ExpandOperationName(project, name string) string {
	if !strings.HasPrefix(name, "projects/") {
		if !strings.HasPrefix(name, "operations/") {
			name = path.Join("operations/", name)
		}
		name = path.Join("projects", project, name)
	}
	return name
}

// ParseFlags calls parse on flags and collects non-flag arguments until there
// are no non-flag arguments remaining.  This makes it possible to handle mixed
// flag and non-flag arguments.
func ParseFlags(flags *flag.FlagSet, arguments []string) []string {
	var nonFlags []string
	for {
		flags.Parse(arguments)
		if flags.NArg() == 0 {
			return nonFlags
		}
		nonFlags = append(nonFlags, flags.Arg(0))
		arguments = flags.Args()[1:]
	}
}

type MapFlagValue struct {
	Values map[string]string
}

func (m *MapFlagValue) String() string {
	return fmt.Sprintf("%v", m.Values)
}

func (m *MapFlagValue) Set(input string) error {
	if i := strings.Index(input, "="); i >= 0 {
		m.Values[input[0:i]] = input[i+1:]
	} else {
		m.Values[input] = ""
	}
	return nil
}

// PipelineExecutionError is an error returned by the Genomics API
// during a pipeline execution
type PipelineExecutionError genomics.Status

func (err PipelineExecutionError) Error() string {
	reason := code.Code_name[int32(err.Code)]
	if reason == "" {
		reason = fmt.Sprintf("unknown error code %d", err.Code)
	}
	return fmt.Sprintf("executing pipeline: %s (reason: %s)", err.Message, reason)
}

// IsRetriable indicates if the user should retry the operation after receiving
// the current error.
func (err PipelineExecutionError) IsRetriable() bool {
	switch code.Code_name[int32(err.Code)] {
	case "UNAVAILABLE", "ABORTED":
		return true
	default:
		return false
	}
}
