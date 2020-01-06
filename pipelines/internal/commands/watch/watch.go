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
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/googlegenomics/pipelines-tools/pipelines/internal/common"
	genomics "google.golang.org/api/genomics/v2alpha1"
)

var (
	flags = flag.NewFlagSet("", flag.ExitOnError)

	actions = flags.Bool("actions", false, "show action details")
	details = flags.Bool("details", false, "show event details")
)

func Invoke(ctx context.Context, service *genomics.Service, project string, arguments []string) error {
	names := common.ParseFlags(flags, arguments)
	if len(names) < 1 {
		return errors.New("missing operation name")
	}

	name := common.ExpandOperationName(project, names[0])
	result, err := watch(ctx, service, project, name)
	if err != nil {
		return fmt.Errorf("watching pipeline: %v", err)
	}

	if status, ok := result.(*genomics.Status); ok {
		return common.PipelineExecutionError(*status)
	}

	fmt.Println("Pipeline execution completed")
	return nil
}

func watch(ctx context.Context, service *genomics.Service, project, name string) (interface{}, error) {
	lro, err := service.Projects.Operations.Get(name).Context(ctx).Do()
	if err != nil {
		return nil, fmt.Errorf("getting operation status: %v", err)
	}

	var metadata genomics.Metadata
	if err := json.Unmarshal(lro.Metadata, &metadata); err != nil {
		return nil, fmt.Errorf("parsing metadata: %v", err)
	}

	if *actions {
		encoded, err := json.MarshalIndent(metadata.Pipeline.Actions, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("encoding actions: %v", err)
		}
		fmt.Printf("%s\n", encoded)
	}

	var events []*genomics.Event
	scan := func(ctx context.Context) (bool, interface{}, error) {
		lro, err := service.Projects.Operations.Get(name).Context(ctx).Do()
		if err != nil {
			return false, nil, fmt.Errorf("getting operation status: %v", err)
		}

		var metadata genomics.Metadata
		if err := json.Unmarshal(lro.Metadata, &metadata); err != nil {
			return false, nil, fmt.Errorf("parsing metadata: %v", err)
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
				return true, lro.Error, nil
			}
			return true, lro.Response, nil
		}
		return false, nil, nil
	}

	if metadata.PubSubTopic != "" {
		sub, err := newPubSubSubscription(ctx, project, metadata.PubSubTopic)
		if err != nil {
			return nil, fmt.Errorf("creating Pub/Sub subscription: %v", err)
		}
		defer sub.Delete(ctx)

		// Check if the operation finished before creating the subscription.
		if done, result, err := scan(ctx); err != nil || done {
			return result, err
		}

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		var response interface{}
		var receiverErr error
		var receiverLock sync.Mutex
		err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			receiverLock.Lock()
			defer receiverLock.Unlock()
			m.Ack()

			exit := func(r interface{}, err error) {
				if ctx.Err() != nil {
					return
				}
				response = r
				receiverErr = err
				cancel()
			}

			if done, result, err := scan(ctx); err != nil || done {
				exit(result, err)
			}
		})
		if err != nil && err != context.Canceled {
			return nil, fmt.Errorf("receiving message: %v", err)
		}
		return response, receiverErr
	} else {
		if lro.Done {
			if lro.Error != nil {
				return lro.Error, nil
			}
			return lro.Response, nil
		}

		const initialDelay = 5 * time.Second
		delay := initialDelay
		for {
			time.Sleep(delay)
			delay = time.Duration(float64(delay) * 1.5)
			if limit := time.Minute; delay > limit {
				delay = limit
			}

			if done, result, err := scan(ctx); err != nil || done {
				return result, err
			}
		}
	}
}

func newPubSubSubscription(ctx context.Context, projectID, topic string) (*pubsub.Subscription, error) {
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("creating a Pub/Sub client: %v", err)
	}

	var id uint64
	if err := binary.Read(rand.Reader, binary.LittleEndian, &id); err != nil {
		return nil, fmt.Errorf("generating subscription name: %v", err)
	}

	el := strings.Split(topic, "/")
	if len(el) < 4 {
		return nil, fmt.Errorf("invalid Pub/Sub topic")
	}

	sub, err := client.CreateSubscription(ctx, fmt.Sprintf("s%d", id), pubsub.SubscriptionConfig{
		Topic:            client.TopicInProject(el[3], el[1]),
		AckDeadline:      10 * time.Second,
		ExpirationPolicy: 25 * time.Hour,
	})
	if err != nil {
		return nil, fmt.Errorf("creating subscription: %v", err)
	}
	return sub, nil
}
