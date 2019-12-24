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

// Package export provides a sub-tool for exporting pipelines to BigQuery.
package export

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/googlegenomics/pipelines-tools/export/export"
	genomics "google.golang.org/api/genomics/v2alpha1"
)

var (
	flags = flag.NewFlagSet("", flag.ExitOnError)

	filter  = flags.String("filter", "", "the export filter")
	dataset = flags.String("dataset", "", "the dataset to export to which must already exist")
	table   = flags.String("table", "", "the table to export to")
	update  = flags.Bool("update", true, "only export operations newer than those already exported")
)

func Invoke(ctx context.Context, service *genomics.Service, project string, arguments []string) error {
	flags.Parse(arguments)

	if *dataset == "" || *table == "" {
		return errors.New("dataset and table are required")
	}

	e, err := export.NewExporter(ctx, project, *filter, *dataset, *table, *update, func(filter string, t time.Time) string {
		return export.CombineTerms(fmt.Sprintf("metadata.createTime > %q", t.Format(time.RFC3339Nano)), filter, "%s AND (%s)")
	})
	if err != nil {
		return fmt.Errorf("creating exporter: %v", err)
	}

	path := fmt.Sprintf("projects/%s/operations", project)
	call := service.Projects.Operations.List(path).Context(ctx).PageSize(256).Filter(e.Filter)
	err = call.Pages(ctx, func(resp *genomics.ListOperationsResponse) error {
		e.StartPage()

		for _, operation := range resp.Operations {
			var metadata genomics.Metadata
			if err := json.Unmarshal(operation.Metadata, &metadata); err != nil {
				return fmt.Errorf("unmarshalling operation (after %d operations): %v", e.Count, err)
			}

			pipeline, err := json.Marshal(metadata.Pipeline)
			if err != nil {
				return fmt.Errorf("marshalling pipeline (after %d operations): %v", e.Count, err)
			}
			resources := metadata.Pipeline.Resources

			r := export.Row{
				Name:       operation.Name,
				Done:       operation.Done,
				CreateTime: export.ParseTimestamp(metadata.CreateTime).Timestamp,
				StartTime:  export.ParseTimestamp(metadata.StartTime),
				EndTime:    export.ParseTimestamp(metadata.EndTime),

				Pipeline:    string(pipeline),
				Regions:     resources.Regions,
				Zones:       resources.Zones,
				MachineType: resources.VirtualMachine.MachineType,
				Preemptible: resources.VirtualMachine.Preemptible,
			}

			if operation.Error != nil {
				r.Error = &export.Status{
					Message: operation.Error.Message,
					Code:    operation.Error.Code,
				}
			}

			for k, v := range metadata.Labels {
				r.Labels = append(r.Labels, export.Label{Key: k, Value: v})
			}

			for _, e := range metadata.Events {
				r.Events = append(r.Events, export.Event{
					Timestamp:   export.ParseTimestamp(e.Timestamp).Timestamp,
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
		return fmt.Errorf("exporting operations: %v", err)
	}

	e.Finish()
	return nil
}
