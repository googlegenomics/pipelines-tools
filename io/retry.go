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
	"log"
	"os"
	"os/exec"
	"strings"
	"syscall"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <command> <arguments>\n", os.Args[0])
		os.Exit(1)
	}

	tool := os.Args[1]

	for attempts := 3; attempts > 0; attempts-- {
		command := exec.Command(tool, os.Args[2:]...)
		command.Stderr = os.Stderr
		command.Stdout = os.Stdout

		if err := command.Run(); err != nil {
			if !isRetriable(tool, err) {
				os.Exit(err.(*exec.ExitError).Sys().(syscall.WaitStatus).ExitStatus())
			}
			log.Printf("%q exited with a retriable error: %v", tool, err)
			continue
		}

		break
	}
}

func isRetriable(tool string, err error) bool {
	if tool == "gsutil" {
		// Requests to the local metadata service can fail transiently.
		return strings.Contains(err.Error(), `Your "GCE" credentials are invalid`)
	}
	return false
}
