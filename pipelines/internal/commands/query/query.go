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

// Package query provides a sub-tool for querying running pipelines.
package query

import (
	"context"
	"flag"
	"fmt"
	"strings"

	genomics "google.golang.org/api/lifesciences/v2beta"
)

var (
	flags = flag.NewFlagSet("", flag.ExitOnError)

	filter = flags.String("filter", "", "the query filter")
	limit  = flags.Uint("limit", 32, "the maximum number of operations to list")
	all    = flags.Bool("all", false, "show all operations (when false, show only running operations)")
)

func Invoke(ctx context.Context, service *genomics.Service, project, location string, arguments []string) error {
	flags.Parse(arguments)

	path := fmt.Sprintf("projects/%s/locations/%s/operations", project, location)
	call := service.Projects.Locations.Operations.List(path).Context(ctx)

	if !*all {
		*filter = strings.Join([]string{*filter, "done=false"}, " ")
	}

	if *filter != "" {
		call = call.Filter(*filter)
	}

	if *limit == 0 {
		return nil
	}

	var pageToken string
	var count uint
	for {
		resp, err := call.PageToken(pageToken).Do()
		if err != nil {
			return err
		}

		for _, operation := range resp.Operations {
			fmt.Println(operation.Name)
			count++
			if count == *limit {
				return nil
			}
		}

		if resp.NextPageToken == "" {
			return nil
		}

		pageToken = resp.NextPageToken
	}
}
