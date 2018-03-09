// Package common provides functions used by multiple commands.
package common

import (
	"flag"
	"path"
	"strings"
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
