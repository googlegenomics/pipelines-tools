// Package operations provides support for long running operations.
package operations

import (
	"path"
	"strings"
)

// ExpandName adds the project and operations prefixes to name if required.
func ExpandName(project, name string) string {
	if !strings.HasPrefix(name, "projects/") {
		if !strings.HasPrefix(name, "operations/") {
			name = path.Join("operations/", name)
		}
		name = path.Join("projects", project, name)
	}
	return name
}
