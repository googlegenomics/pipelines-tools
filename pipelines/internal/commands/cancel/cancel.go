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
	"fmt"
	"net/http"

	"github.com/googlegenomics/pipelines-tools/pipelines/internal/common"
	"google.golang.org/api/googleapi"
	genomics "google.golang.org/api/lifesciences/v2beta"
)

func Invoke(ctx context.Context, service *genomics.Service, project, location string, arguments []string) error {
	if len(arguments) < 1 {
		return errors.New("missing operation name")
	}

	name := common.ExpandOperationName(project, location, arguments[0])
	req := &genomics.CancelOperationRequest{}
	if _, err := service.Projects.Locations.Operations.Cancel(name, req).Context(ctx).Do(); err != nil {
		if err, ok := err.(*googleapi.Error); ok && err.Code == http.StatusNotFound {
			return fmt.Errorf("operation %q not found", name)
		}
		return err
	}

	fmt.Println("Operation cancelled")
	return nil
}
