// Package common provides functions used by multiple commands.
package common

import (
	"flag"
	"fmt"
	"path"
	"strings"

	genomics "google.golang.org/api/lifesciences/v2beta"
	"google.golang.org/genproto/googleapis/rpc/code"
)

// ExpandOperationName adds the project, location and operations prefixes to name (if
// they are not already present).
func ExpandOperationName(project, location, name string) string {
	if !strings.HasPrefix(name, "projects/") {
		if !strings.HasPrefix(name, "locations/") {
			if !strings.HasPrefix(name, "operations/") {
				name = path.Join("operations/", name)
			}
			name = path.Join("locations", location, name)
		}
		name = path.Join("projects", project, name)
	}
	return name
}

//func ExpandOperationName(prefixes, values []string, name string) (string, error) {
// 	if len(prefixes) != len(values) {
//		return name, fmt.Errorf("lengths of prefixes and values not equal")
//	}
//	if len(prefixes) == 0 {
//		return name, nil
//	}
//	name, _ := ExpandOperationName(prefixes[1:], values[1:], name)
//	if !strings.HasPrefix(name, path.Clean(prefixes[0])+"/") {
//		name = path.Join(prefixes[0], values[0], name)
//	}
//	return name, nil
//}

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

var fatalErrorCodes = map[code.Code]bool{
	code.Code_FAILED_PRECONDITION: true,
	code.Code_INVALID_ARGUMENT:    true,
	code.Code_ALREADY_EXISTS:      true,
	code.Code_NOT_FOUND:           true,
	code.Code_OUT_OF_RANGE:        true,
	code.Code_PERMISSION_DENIED:   true,
	code.Code_UNAUTHENTICATED:     true,
	code.Code_UNIMPLEMENTED:       true,
}

// IsRetriable indicates if the user should retry the operation after receiving
// the current error.
func (err PipelineExecutionError) IsRetriable() bool {
	return !fatalErrorCodes[code.Code(err.Code)]
}
