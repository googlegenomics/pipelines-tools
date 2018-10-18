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

// Package export provides helpers for exporting pipelines to BigQuery.
package export

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

type Exporter struct {
	Filter string
	Count  int

	bq     *bigquery.Client
	table  *bigquery.Table
	schema bigquery.Schema

	buffer  bytes.Buffer
	encoder *json.Encoder
}

func NewExporter(ctx context.Context, project, filter, datasetName, tableName string, update bool, addTimestamp func(string, time.Time) string) (*Exporter, error) {
	bq, err := bigquery.NewClient(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("creating BigQuery client: %v", err)
	}

	dataset := bq.Dataset(datasetName)
	if _, err := dataset.Metadata(ctx); err != nil {
		return nil, fmt.Errorf("looking up dataset: %v", err)
	}

	schema, err := bigquery.InferSchema(Row{})
	if err != nil {
		return nil, fmt.Errorf("inferring schema: %v", err)
	}

	f := filter
	table := dataset.Table(tableName)
	if _, err := table.Metadata(ctx); err != nil {
		if err := table.Create(ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
			return nil, fmt.Errorf("creating table: %v", err)
		}
	} else if update {
		timestamp, err := latestTimestamp(ctx, bq, project, datasetName, tableName)
		if err != nil {
			return nil, fmt.Errorf("retrieving latest timestamp: %v", err)
		}
		if timestamp != nil {
			f = addTimestamp(f, *timestamp)
		}
	}

	fmt.Printf("Exporting operations")

	return &Exporter{Filter: f, bq: bq, table: table, schema: schema}, nil
}

func (e *Exporter) StartPage() {
	e.buffer.Reset()
	e.encoder = json.NewEncoder(&e.buffer)
	e.encoder.SetEscapeHTML(false)
}

func (e *Exporter) Encode(r *Row) error {
	e.Count++
	return e.encoder.Encode(r)
}

func (e *Exporter) FinishPage(ctx context.Context) error {
	fmt.Printf(".")

	if e.buffer.Len() == 0 {
		return nil
	}

	source := bigquery.NewReaderSource(&e.buffer)
	source.Schema = e.schema
	source.SourceFormat = bigquery.JSON
	loader := e.table.LoaderFrom(source)

	job, err := loader.Run(ctx)
	if err != nil {
		return fmt.Errorf("running loader (after %d operations): %v", e.Count, err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("waiting for job (after %d operations): %v", e.Count, err)
	}

	if err := status.Err(); err != nil {
		for _, e := range status.Errors {
			fmt.Println(e)
		}
		return fmt.Errorf("job status (after %d operations): %v", e.Count, err)
	}

	return nil
}

func (e *Exporter) Finish() {
	fmt.Printf("done\n%d operations exported\n", e.Count)
}

type Row struct {
	Name  string
	Done  bool
	Error *Status `bigquery:",nullable"`

	// The raw pipeline JSON.
	Pipeline string

	Labels []Label
	Events []Event

	CreateTime         time.Time
	StartTime, EndTime bigquery.NullTimestamp

	// Additional fields pulled out of the pipeline for convenience.
	Regions     []string
	Zones       []string
	MachineType string
	Preemptible bool
}

type Status struct {
	Message string
	Code    int64
}

type Event struct {
	Timestamp   time.Time
	Description string
}

type Label struct {
	Key, Value string
}

func ParseTimestamp(ts string) bigquery.NullTimestamp {
	t, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return bigquery.NullTimestamp{}
	}
	return bigquery.NullTimestamp{
		Timestamp: t,
		Valid:     true,
	}
}

func CombineTerms(req, opt, format string) string {
	if opt == "" {
		return req
	}
	return fmt.Sprintf(format, req, opt)
}

func latestTimestamp(ctx context.Context, bq *bigquery.Client, project, dataset, table string) (*time.Time, error) {
	// Query in micros otherwise BigQuery returns the timestamp as a double.
	q := bq.Query(fmt.Sprintf("SELECT UNIX_MICROS(MAX(CreateTime)) FROM `%s.%s.%s`", project, dataset, table))
	job, err := q.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("running query: %v", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("waiting for query: %v", err)
	}
	if err := status.Err(); err != nil {
		return nil, fmt.Errorf("query status: %v", err)
	}
	it, err := job.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading query: %v", err)
	}

	var v []bigquery.Value
	err = it.Next(&v)
	if err == iterator.Done {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting query data: %v", err)
	}

	micros, ok := v[0].(int64)
	if !ok {
		return nil, fmt.Errorf("unexpected timestamp type: %T", v[0])
	}
	t := time.Unix(0, micros*int64(time.Microsecond)).UTC()
	return &t, nil
}
