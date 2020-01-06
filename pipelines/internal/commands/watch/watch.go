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

// Package watch provides a sub tool for watching a running pipeline.
package watch

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/googlegenomics/pipelines-tools/pipelines/internal/common"
	genomics "google.golang.org/api/lifesciences/v2beta"
)

var (
	flags = flag.NewFlagSet("", flag.ExitOnError)

	actions = flags.Bool("actions", false, "show action details")
	details = flags.Bool("details", false, "show event details")
)

func Invoke(ctx context.Context, service *genomics.Service, project string, location string, arguments []string) error {
	names := common.ParseFlags(flags, arguments)
	if len(names) < 1 {
		return errors.New("missing operation name")
	}

	name := common.ExpandOperationName(project, location, names[0])
	result, err := watch(ctx, service, name)
	if err != nil {
		return fmt.Errorf("watching pipeline: %v", err)
	}

	if status, ok := result.(*genomics.Status); ok {
		return common.PipelineExecutionError(*status)
	}

	fmt.Println("Pipeline execution completed")
	return nil
}

func watch(ctx context.Context, service *genomics.Service, name string) (interface{}, error) {
	var events []*genomics.Event
	const initialDelay = 5 * time.Second
	delay := initialDelay
	fmt.Println(name)
	for {
		lro, err := service.Projects.Locations.Operations.Get(name).Context(ctx).Do()
		if err != nil {
			return nil, fmt.Errorf("getting operation status: %v", err)
		}

		var metadata genomics.Metadata
		if err := json.Unmarshal(lro.Metadata, &metadata); err != nil {
			return nil, fmt.Errorf("parsing metadata: %v", err)
		}

		if *actions {
			*actions = false
			encoded, err := json.MarshalIndent(metadata.Pipeline.Actions, "", "  ")
			if err != nil {
				return nil, fmt.Errorf("encoding actions: %v", err)
			}
			fmt.Printf("%s\n", encoded)
		}

		if len(events) != len(metadata.Events) {
			for i := len(metadata.Events) - len(events) - 1; i >= 0; i-- {
				timestamp, _ := time.Parse(time.RFC3339Nano, metadata.Events[i].Timestamp)
				fmt.Println(timestamp.Format("15:04:05"), metadata.Events[i].Description)

				if *details {
					encoded, err := json.MarshalIndent(metadata.Events[i], "", " ")
					if err != nil {
						return nil, fmt.Errorf("encoding event: %v", err)
					}
					fmt.Printf("%s\n", encoded)
				}
			}
			events = metadata.Events
			delay = initialDelay
		}

		if lro.Done {
			if lro.Error != nil {
				return lro.Error, nil
			}
			return lro.Response, nil
		}

		time.Sleep(delay)
		delay = time.Duration(float64(delay) * 1.5)
		if limit := time.Minute; delay > limit {
			delay = limit
		}
	}
}
