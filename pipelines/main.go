// Copyright 2018 Google Inc.
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

// This tool provides several sub-tools for working with the pipelines API.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/googlegenomics/pipelines-tools/client"
	"github.com/googlegenomics/pipelines-tools/pipelines/internal/commands/cancel"
	"github.com/googlegenomics/pipelines-tools/pipelines/internal/commands/export"
	"github.com/googlegenomics/pipelines-tools/pipelines/internal/commands/query"
	"github.com/googlegenomics/pipelines-tools/pipelines/internal/commands/run"
	"github.com/googlegenomics/pipelines-tools/pipelines/internal/commands/watch"

	genomics "google.golang.org/api/genomics/v2alpha1"
)

var (
	project  = flag.String("project", defaultProject(), "the cloud project name")
	basePath = flag.String("api", "", "the API base to use")

	commands = map[string]func(context.Context, *genomics.Service, string, []string) error{
		"run":    run.Invoke,
		"cancel": cancel.Invoke,
		"query":  query.Invoke,
		"watch":  watch.Invoke,
		"export": export.Invoke,
	}
)

func main() {
	flag.Parse()

	if flag.NArg() == 0 {
		names := make([]string, 0, len(commands))
		for name := range commands {
			names = append(names, name)
		}
		exitf("Missing command name: expecting one of %s", names)
	}

	if *project == "" {
		exitf("You must specify a project with --project")
	}

	command := flag.Arg(0)
	invoke := commands[command]
	if invoke == nil {
		exitf("Unknown command %q", command)
	}

	ctx := context.Background()
	service, err := newService(ctx, *basePath)
	if err != nil {
		exitf("Failed to create service: %v", err)
	}

	if err := invoke(ctx, service, *project, flag.Args()[1:]); err != nil {
		exitf("%q: %v", command, err)
	}
}

func newService(ctx context.Context, basePath string) (*genomics.Service, error) {
	c, err := client.NewClient(ctx, genomics.GenomicsScope, basePath)
	if err != nil {
		return nil, fmt.Errorf("creating client: %v", err)
	}

	service, err := genomics.New(c)
	if err != nil {
		return nil, fmt.Errorf("creating service object: %v", err)
	}
	if basePath != "" {
		service.BasePath = basePath
	}
	return service, nil
}

func exitf(format string, arguments ...interface{}) {
	fmt.Fprintf(os.Stderr, format, arguments...)
	fmt.Fprintln(os.Stderr)
	os.Exit(1)
}

func defaultProject() string {
	return os.Getenv("GOOGLE_CLOUD_PROJECT")
}
