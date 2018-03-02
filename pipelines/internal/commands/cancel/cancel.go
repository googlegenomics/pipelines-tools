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

// Package cancel provides a sub-tool for cancelling running pipelines.
package cancel

import (
	"context"
	"errors"
	"flag"
	"fmt"

	genomics "google.golang.org/api/genomics/v2alpha1"
	"google.golang.org/api/googleapi"
)

var (
	flags flag.FlagSet

	id = flags.String("id", "", "the operation id to cancel")
)

func Invoke(ctx context.Context, service *genomics.Service, project string, arguments []string) error {
	flags.Parse(arguments)

	if *id == "" {
		return errors.New("the id flag is required")
	}

	name := fmt.Sprintf("projects/%s/operations/%s", project, *id)

	req := &genomics.CancelOperationRequest{}
	if _, err := service.Projects.Operations.Cancel(name, req).Context(ctx).Do(); err != nil {
		if err, ok := err.(*googleapi.Error); ok {
			return fmt.Errorf("starting pipeline: %q: %q", err.Message, err.Body)
		}
		return err
	}

	fmt.Println("Operation cancelled")
	return nil
}
