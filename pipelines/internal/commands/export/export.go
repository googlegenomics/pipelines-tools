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

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	genomics "google.golang.org/api/genomics/v2alpha1"
)

var (
	flags = flag.NewFlagSet("", flag.ExitOnError)

	filter      = flags.String("filter", "", "the export filter")
	datasetName = flags.String("dataset", "", "the dataset to export to which must already exist")
	tableName   = flags.String("table", "", "the table to export to")
)

type row struct {
	Name  string
	Done  bool
	Error *status `bigquery:",nullable"`

	// The raw pipeline JSON.
	Pipeline string

	Labels []label
	Events []event

	CreateTime         civil.DateTime
	StartTime, EndTime bigquery.NullDateTime

	// Additional fields pulled out of the pipeline for convenience.
	Regions     []string
	Zones       []string
	MachineType string
	Preemptible bool
}

type status struct {
	Message string
	Code    int64
}

type event struct {
	Timestamp   civil.DateTime
	Description string
}

type label struct {
	Key, Value string
}

func Invoke(ctx context.Context, service *genomics.Service, project string, arguments []string) error {
	flags.Parse(arguments)

	if *datasetName == "" || *tableName == "" {
		return errors.New("dataset and table are required")
	}

	path := fmt.Sprintf("projects/%s/operations", project)
	call := service.Projects.Operations.List(path).Context(ctx)
	call.PageSize(256)

	if *filter != "" {
		call = call.Filter(*filter)
	}

	bq, err := bigquery.NewClient(ctx, project)
	if err != nil {
		return fmt.Errorf("creating BigQuery client: %v", err)
	}

	dataset := bq.Dataset(*datasetName)
	if _, err := dataset.Metadata(ctx); err != nil {
		return fmt.Errorf("looking up dataset: %v", err)
	}

	schema, err := bigquery.InferSchema(row{})
	if err != nil {
		return fmt.Errorf("inferring schema: %v", err)
	}

	table := dataset.Table(*tableName)
	if _, err := table.Metadata(ctx); err != nil {
		if err := table.Create(ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
			return fmt.Errorf("creating table: %v", err)
		}
	}

	uploader := table.Uploader()

	fmt.Printf("Exporting operations")

	var count int
	var pageToken string
	for {
		resp, err := call.PageToken(pageToken).Do()
		if err != nil {
			return fmt.Errorf("calling list (after %d operations): %v", count, err)
		}

		fmt.Printf(".")

		var savers []*bigquery.StructSaver
		for _, operation := range resp.Operations {
			var metadata genomics.Metadata
			if err := json.Unmarshal(operation.Metadata, &metadata); err != nil {
				return fmt.Errorf("unmarshalling operation (after %d operations): %v", count, err)
				return err
			}

			pipeline, err := json.Marshal(metadata.Pipeline)
			if err != nil {
				return fmt.Errorf("marshalling pipeline (after %d operations): %v", count, err)
			}
			resources := metadata.Pipeline.Resources

			r := row{
				Name:       operation.Name,
				Done:       operation.Done,
				CreateTime: parseTimestamp(metadata.CreateTime).DateTime,
				StartTime:  parseTimestamp(metadata.StartTime),
				EndTime:    parseTimestamp(metadata.EndTime),

				Pipeline:    string(pipeline),
				Regions:     resources.Regions,
				Zones:       resources.Zones,
				MachineType: resources.VirtualMachine.MachineType,
				Preemptible: resources.VirtualMachine.Preemptible,
			}

			if operation.Error != nil {
				r.Error = &status{
					Message: operation.Error.Message,
					Code:    operation.Error.Code,
				}
			}

			for k, v := range metadata.Labels {
				r.Labels = append(r.Labels, label{Key: k, Value: v})
			}

			for _, e := range metadata.Events {
				r.Events = append(r.Events, event{
					Timestamp:   parseTimestamp(e.Timestamp).DateTime,
					Description: e.Description,
				})
			}

			savers = append(savers, &bigquery.StructSaver{
				Struct:   r,
				InsertID: operation.Name,
				Schema:   schema,
			})
			count++
		}

		if err := uploader.Put(ctx, savers); err != nil {
			return fmt.Errorf("uploading rows (after %d operations): %v", count, err)
		}

		if resp.NextPageToken == "" {
			fmt.Printf("done\n%d operations exported\n", count)
			return nil
		}

		pageToken = resp.NextPageToken
	}
}

func parseTimestamp(ts string) bigquery.NullDateTime {
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return bigquery.NullDateTime{}
	}
	return bigquery.NullDateTime{
		DateTime: civil.DateTimeOf(t),
		Valid:    true,
	}
}
