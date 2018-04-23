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

func (status PipelineExecutionError) Error() string {
	return fmt.Sprintf( "executing pipeline: %d : %s", status.Code, status.Message)
}

// Map that indicates which Genomics errors are fatal and
// for which the user should retry the operation.
// Errors that are not present or have the value "false" are fatal and
// should not be retried.
var retriableErrors map[string]bool= map[string]bool{
	"UNAVAILABLE": true,
	"ABORTED" : true,
	"FAILED_PRECONDITION" : false,
	"CANCELLED": false,
	"INVALID_ARGUMENT" : false,
}

// IsRetriable indicates if the user should retry the operation after receiving
// the current error.
func (status PipelineExecutionError) IsRetriable() bool {
	return retriableErrors[code.Code_name[int32(status.Code)]]
}
