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

// This tool exports pipelines operations to BigQuery.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/googlegenomics/pipelines-tools/client"
	"github.com/googlegenomics/pipelines-tools/export/export"

	genomics "google.golang.org/api/genomics/v1alpha2"
)

var (
	project  = flag.String("project", defaultProject(), "the cloud project name")
	basePath = flag.String("api", "", "the API base to use")

	filter  = flag.String("filter", "", "the export filter")
	dataset = flag.String("dataset", "", "the dataset to export to which must already exist")
	table   = flag.String("table", "", "the table to export to")
	update  = flag.Bool("update", true, "only export operations newer than those already exported")
)

func main() {
	flag.Parse()

	if *project == "" {
		exitf("You must specify a project with --project")
	}

	if *dataset == "" || *table == "" {
		exitf("You must specify a dataset and table with --dataset and --table")
	}

	ctx := context.Background()
	service, err := newService(ctx, *basePath)
	if err != nil {
		exitf("Failed to create service: %v", err)
	}

	e, err := export.NewExporter(ctx, *project, *filter, *dataset, *table, *update, func(filter string, t time.Time) string {
		return export.CombineTerms(fmt.Sprintf("createTime >= %d", t.Unix()+1), filter, "%s AND %s")
	})
	if err != nil {
		exitf("Failed to initialize exporter: %v", err)
	}

	f := export.CombineTerms("projectId = "+*project, e.Filter, "%s AND %s")

	call := service.Operations.List("operations").Context(ctx).PageSize(256).Filter(f)
	err = call.Pages(ctx, func(resp *genomics.ListOperationsResponse) error {
		e.StartPage()

		for _, operation := range resp.Operations {
			var metadata genomics.OperationMetadata
			if err := json.Unmarshal(operation.Metadata, &metadata); err != nil {
				return fmt.Errorf("unmarshalling operation (after %d operations): %v", e.Count, err)
			}

			if metadata.Request == nil {
				continue
			}

			var t struct {
				Type string `json:"@type"`
			}
			if err := json.Unmarshal(metadata.Request, &t); err != nil {
				return fmt.Errorf("unmarshalling request type (after %d operations): %v", e.Count, err)
			}
			if !strings.HasSuffix(t.Type, ".RunPipelineRequest") {
				continue
			}

			var request genomics.RunPipelineRequest
			if err := json.Unmarshal(metadata.Request, &request); err != nil {
				return fmt.Errorf("unmarshalling request (after %d operations): %v", e.Count, err)
			}

			var zones []string
			var preemptible bool
			if request.EphemeralPipeline != nil && request.EphemeralPipeline.Resources != nil {
				zones = request.EphemeralPipeline.Resources.Zones
				preemptible = request.EphemeralPipeline.Resources.Preemptible
			}
			if request.PipelineArgs != nil && request.PipelineArgs.Resources != nil {
				zones = request.PipelineArgs.Resources.Zones
				preemptible = request.PipelineArgs.Resources.Preemptible
			}

			r := export.Row{
				Name:       operation.Name,
				Done:       operation.Done,
				CreateTime: export.ParseTimestamp(metadata.CreateTime).Timestamp,
				StartTime:  export.ParseTimestamp(metadata.StartTime),
				EndTime:    export.ParseTimestamp(metadata.EndTime),

				Pipeline:    string(metadata.Request),
				Zones:       zones,
				Preemptible: preemptible,
			}

			if metadata.RuntimeMetadata != nil {
				var runtime genomics.RuntimeMetadata
				if err := json.Unmarshal(metadata.RuntimeMetadata, &runtime); err != nil {
					return fmt.Errorf("unmarshalling request (after %d operations): %v", e.Count, err)
				}

				if runtime.ComputeEngine != nil {
					parts := strings.Split(runtime.ComputeEngine.MachineType, "/")
					r.MachineType = parts[1]
				}
			}

			if operation.Error != nil {
				r.Error = &export.Status{
					Message: operation.Error.Message,
					Code:    operation.Error.Code,
				}
			}

			if request.PipelineArgs != nil {
				for k, v := range request.PipelineArgs.Labels {
					r.Labels = append(r.Labels, export.Label{Key: k, Value: v})
				}
			}

			for _, e := range metadata.Events {
				r.Events = append(r.Events, export.Event{
					Timestamp:   export.ParseTimestamp(e.StartTime).Timestamp,
					Description: e.Description,
				})
			}

			if err := e.Encode(&r); err != nil {
				return fmt.Errorf("encoding row (after %d operations): %v", e.Count, err)
			}
		}

		if err := e.FinishPage(ctx); err != nil {
			return fmt.Errorf("finishing page (after %d operations): %v", e.Count, err)
		}

		return nil
	})
	if err != nil {
		exitf("Failed to export operations: %v", err)
	}

	e.Finish()
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
