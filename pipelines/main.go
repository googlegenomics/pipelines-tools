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
	"crypto/tls"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/googlegenomics/pipelines-tools/pipelines/internal/commands/cancel"
	"github.com/googlegenomics/pipelines-tools/pipelines/internal/commands/query"
	"github.com/googlegenomics/pipelines-tools/pipelines/internal/commands/run"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	genomics "google.golang.org/api/genomics/v2alpha1"
)

var (
	project  = flag.String("project", "", "the cloud project name")
	basePath = flag.String("base_path", "", "the base API path to use")

	commands = map[string]func(context.Context, *genomics.Service, string, []string) error{
		"run":    run.Invoke,
		"cancel": cancel.Invoke,
		"query":  query.Invoke,
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

func exitf(format string, arguments ...interface{}) {
	fmt.Fprintf(os.Stderr, format, arguments...)
	fmt.Fprintln(os.Stderr)
	os.Exit(1)
}

func newService(ctx context.Context, basePath string) (*genomics.Service, error) {
	// When connecting to a local server (for Google developers only) disable SSL
	// verification since the certificates are not easily verifiable.
	if strings.HasPrefix(basePath, "https://localhost:") {
		ctx = context.WithValue(ctx, oauth2.HTTPClient, &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		})
	}

	client, err := google.DefaultClient(ctx, genomics.GenomicsScope)
	if err != nil {
		return nil, fmt.Errorf("creating authenticated client: %v", err)
	}

	service, err := genomics.New(client)
	if err != nil {
		return nil, fmt.Errorf("creating service object: %v", err)
	}
	if basePath != "" {
		service.BasePath = basePath
	}
	return service, nil
}
