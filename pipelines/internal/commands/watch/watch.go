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
	"cloud.google.com/go/pubsub"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/googlegenomics/pipelines-tools/pipelines/internal/common"
	genomics "google.golang.org/api/genomics/v2alpha1"
)

var (
	flags = flag.NewFlagSet("", flag.ExitOnError)

	actions = flags.Bool("actions", false, "show action details")
	details = flags.Bool("details", false, "show event details")
	topic   = flags.String("topic", "", "Pub/Sub topic")
)

func Invoke(ctx context.Context, service *genomics.Service, project string, arguments []string) error {
	names := common.ParseFlags(flags, arguments)
	l := len(names)
	if l < 1 {
		return errors.New("missing operation name")
	}
	if *topic == "" {
		return errors.New("missing Pub/Sub topic name")
	}

	name := common.ExpandOperationName(project, names[0])
	result, err := watch(ctx, service, project, name, *topic)
	if err != nil {
		return fmt.Errorf("watching pipeline: %v", err)
	}

	if status, ok := result.(*genomics.Status); ok {
		return common.PipelineExecutionError(*status)
	}

	fmt.Println("Pipeline execution completed")
	return nil
}

func watch(ctx context.Context, service *genomics.Service, project, name, topic string) (interface{}, error) {
	var events []*genomics.Event

	sub, err := newPubSubSubscription(project, topic)
	if err != nil {
		return nil, fmt.Errorf("creating Pub/Sub subscription: %v", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var response interface{}
	err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		lro, err := service.Projects.Operations.Get(name).Context(ctx).Do()
		if err != nil {
			fmt.Println(fmt.Errorf("getting operation status: %v", err))
		}

		var metadata genomics.Metadata
		if err := json.Unmarshal(lro.Metadata, &metadata); err != nil {
			fmt.Println(fmt.Errorf("parsing metadata: %v", err))
		}

		if *actions {
			*actions = false
			encoded, err := json.MarshalIndent(metadata.Pipeline.Actions, "", "  ")
			if err != nil {
				fmt.Println(fmt.Errorf("encoding actions: %v", err))
			}
			fmt.Printf("%s\n", encoded)
		}

		if len(events) != len(metadata.Events) {
			for i := len(metadata.Events) - len(events) - 1; i >= 0; i-- {
				timestamp, _ := time.Parse(time.RFC3339Nano, metadata.Events[i].Timestamp)
				fmt.Println(timestamp.Format("15:04:05"), metadata.Events[i].Description)

				if *details {
					fmt.Println(string(metadata.Events[i].Details))
				}
			}
			events = metadata.Events
		}

		if lro.Done {
			if lro.Error != nil {
				response = lro.Error
			} else {
				response = lro.Response
			}
			cancel()
		}
		m.Ack()
	})
	if err != context.Canceled {
		return nil, fmt.Errorf("receiving message: %v", err)
	}
	return response, nil
}

func newPubSubSubscription(projectID, topicName string) (*pubsub.Subscription, error) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("creating a Pub/Sub client: %v", err)
	}

	topic, err := client.CreateTopic(ctx, topicName)
	if err != nil {
		return nil, fmt.Errorf("creating Pub/Sub topic: %v", err)
	}

	var subscriptionName uint64
	if err := binary.Read(rand.Reader, binary.LittleEndian, &subscriptionName); err != nil {
		return nil, fmt.Errorf("generating id: %v", err)
	}
	sub, err := client.CreateSubscription(ctx, string(subscriptionName), pubsub.SubscriptionConfig{
		Topic:            topic,
		AckDeadline:      10 * time.Second,
		ExpirationPolicy: 25 * time.Hour,
	})
	if err != nil {
		return nil, fmt.Errorf("creating subscription: %v", err)
	}

	return sub, nil
}
