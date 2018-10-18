// Copyright 2019 Google Inc.
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

// Package genomics provides methods for connecting to the Genomics service.
package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

func NewClient(ctx context.Context, scope, basePath string) (*http.Client, error) {
	var transport robustTransport

	// When connecting to a local server (for Google developers only) disable SSL
	// verification since the certificates are not easily verifiable.
	if strings.HasPrefix(basePath, "https://localhost:") {
		transport.Base.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	ctx = context.WithValue(ctx, oauth2.HTTPClient, &http.Client{Transport: &transport})

	client, err := google.DefaultClient(ctx, scope)
	if err != nil {
		return nil, fmt.Errorf("creating authenticated client: %v", err)
	}

	return client, nil
}

type robustTransport struct {
	Base http.Transport
}

func (rt *robustTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	delay := time.Second

	var errors []string
	for {
		resp, err := rt.roundTrip(req)
		if err == nil {
			return resp, nil
		}
		errors = append(errors, fmt.Sprintf("attempt %d: %v", len(errors)+1, err))
		if len(errors) == 3 {
			return resp, fmt.Errorf("%d failed requests: %v", len(errors), strings.Join(errors, ", "))
		}

		delay *= 2
		time.Sleep(delay)
	}
}

func (rt *robustTransport) roundTrip(req *http.Request) (*http.Response, error) {
	resp, err := rt.Base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	switch resp.StatusCode {
	case http.StatusServiceUnavailable, http.StatusBadGateway, http.StatusGatewayTimeout:
		return nil, fmt.Errorf("retryable HTTP error: %q", resp.Status)
	}
	return resp, err
}
