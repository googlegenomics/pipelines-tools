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

// Package gce provides methods for querying the gce metadata
package gce

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"golang.org/x/crypto/ssh"
)

// GetAuthorizedKeys retrieves the authorized ssh keys from the metadata server running on a GCE instance
func GetAuthorizedKeys() (map[string]bool, error) {
	req, err := http.NewRequest("GET", "http://metadata.google.internal/computeMetadata/v1/project/attributes/ssh-keys", nil)
	if err != nil {
		return nil, fmt.Errorf("building the request: %v", err)
	}
	req.Header.Set("Metadata-Flavor", "Google")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sending request: %v", err)
	}

	keysBytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response: %v", err)
	}

	authorizedKeys := make(map[string]bool)
	for len(keysBytes) > 0 {
		key, _, _, rest, err := ssh.ParseAuthorizedKey(keysBytes)
		if err != nil {
			return nil, fmt.Errorf("parsing authorized key: %v", err)
		}
		authorizedKeys[string(key.Marshal())] = true
		keysBytes = rest
	}
	return authorizedKeys, nil
}
