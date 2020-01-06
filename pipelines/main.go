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
	"time"

	"github.com/googlegenomics/pipelines-tools/pipelines/internal/commands/cancel"
	"github.com/googlegenomics/pipelines-tools/pipelines/internal/commands/export"
	"github.com/googlegenomics/pipelines-tools/pipelines/internal/commands/query"
	"github.com/googlegenomics/pipelines-tools/pipelines/internal/commands/run"
	"github.com/googlegenomics/pipelines-tools/pipelines/internal/commands/watch"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	genomics "google.golang.org/api/lifesciences/v2beta"
)

const (
	genomicsScope = "https://www.googleapis.com/auth/cloud-platform"
)

var (
	project  = flag.String("project", defaultProject(), "the cloud project name")
	location = flag.String("location", "us-central1", "Google cloud location to store the metadata for the operations")
	basePath = flag.String("api", "", "the API base to use")

	commands = map[string]func(context.Context, *genomics.Service, string, string, []string) error{
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

	if err := invoke(ctx, service, *project, *location, flag.Args()[1:]); err != nil {
		exitf("%q: %v", command, err)
	}
}

func exitf(format string, arguments ...interface{}) {
	fmt.Fprintf(os.Stderr, format, arguments...)
	fmt.Fprintln(os.Stderr)
	os.Exit(1)
}

func newService(ctx context.Context, basePath string) (*genomics.Service, error) {
	var transport robustTransport

	// When connecting to a local server (for Google developers only) disable SSL
	// verification since the certificates are not easily verifiable.
	if strings.HasPrefix(basePath, "https://localhost:") {
		transport.Base.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	ctx = context.WithValue(ctx, oauth2.HTTPClient, &http.Client{Transport: &transport})

	client, err := google.DefaultClient(ctx, genomicsScope)
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

func defaultProject() string {
	return os.Getenv("GOOGLE_CLOUD_PROJECT")
}

type robustTransport struct {
	Base http.Transport
}

func (rt *robustTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	delay := time.Second

	var errors []string
	for {
		resp, err := rt.roundTrip(req)
		if err == nil {
			return resp, nil
		}
		errors = append(errors, fmt.Sprintf("attempt %d: %v", len(errors)+1, err))
		if len(errors) == 3 {
			return resp, fmt.Errorf("%d failed requests: %v", len(errors), strings.Join(errors, ", "))
		}

		delay *= 2
		time.Sleep(delay)
	}
}

func (rt *robustTransport) roundTrip(req *http.Request) (*http.Response, error) {
	resp, err := rt.Base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	switch resp.StatusCode {
	case http.StatusServiceUnavailable, http.StatusBadGateway, http.StatusGatewayTimeout:
		return nil, fmt.Errorf("retryable HTTP error: %q", resp.Status)
	}
	return resp, err
}
