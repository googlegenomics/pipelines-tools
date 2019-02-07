// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This tool wraps other known tools to provide additional retry logic.
//
// The intent is that it remain very thin and get smaller over time as fixes
// are pushed into the tools themselves.
package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <command> <arguments>\n", os.Args[0])
		os.Exit(1)
	}

	tool := os.Args[1]
	arguments := os.Args[2:]

	for retries := 0; ; retries++ {
		command := exec.Command(tool, arguments...)
		command.Stderr = os.Stderr
		command.Stdout = os.Stdout

		switch err := command.Run().(type) {
		case *exec.ExitError:
			status := err.Sys().(syscall.WaitStatus).ExitStatus()
			if retries >= 2 {
				os.Exit(status)
			}

			if filepath.Base(tool) == "gsutil" {
				// The gsutil bootstrapping code writes a sticky bit to disk that
				// disables GCE metadata authentication if it has any trouble talking
				// to the metadata endpoint.
				os.Remove(filepath.Join(os.Getenv("HOME"), ".config/gcloud/gce"))
			}

			fmt.Fprintf(os.Stderr, "Execution failed (exit status %d); will retry\n", status)

		case nil:
			return

		default:
			fmt.Fprintln(os.Stderr, "Failed to run command:", err)
			os.Exit(1)
		}
	}
}
