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

// This tool migrates v1alpha2 pipeline definitions to v2alpha1.
package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path"
	"strings"
	"time"

	v1 "google.golang.org/api/genomics/v1alpha2"
	v2 "google.golang.org/api/genomics/v2alpha1"
)

// The 'alpine' variant is about 10x smaller than the default.
const cloudSDKImage = "google/cloud-sdk:alpine"

func main() {
	var input v1.RunPipelineRequest
	if err := json.NewDecoder(os.Stdin).Decode(&input); err != nil {
		exitf("Failed to decode input pipeline: %v", err)
	}

	if input.EphemeralPipeline == nil {
		exitf("An ephemeral pipeline is required as an input")
	}

	arguments := input.PipelineArgs
	if arguments == nil {
		arguments = &v1.RunPipelineArgs{}
	}
	resources := mergeResources(input.EphemeralPipeline.Resources, arguments.Resources)

	var actions []*v2.Action
	mountPoints := make(map[string]string)
	for _, disk := range resources.Disks {
		mountPoints[disk.Name] = disk.MountPoint
	}

	// Each input parameter localizes a file or sets an environment variable.
	environment := make(map[string]string)
	for _, parameter := range input.EphemeralPipeline.InputParameters {
		value := parameterValue(parameter, arguments.Inputs)
		if parameter.LocalCopy == nil {
			environment[parameter.Name] = value
		} else {
			mount := &v2.Mount{
				Disk: parameter.LocalCopy.Disk,
				Path: mountPoints[parameter.LocalCopy.Disk],
			}
			environment[parameter.Name] = path.Join(mount.Path, parameter.LocalCopy.Path)
			action := gsutil("cp", value, environment[parameter.Name])
			action.Mounts = []*v2.Mount{mount}
			action.Labels = map[string]string{"type": "input"}
			actions = append(actions, action)
		}
	}

	// Write the action for the command in the executor.
	if docker := input.EphemeralPipeline.Docker; docker != nil {
		var mounts []*v2.Mount
		for _, disk := range resources.Disks {
			mounts = append(mounts, &v2.Mount{
				Disk: disk.Name,
				Path: disk.MountPoint,
			})
			if disk.Source != "" {
				exitf("Mounting existing disk %q: not supported", disk.Source)
			}
		}
		actions = append(actions, &v2.Action{
			ImageUri: docker.ImageName,
			Commands: []string{"/bin/bash", "-c", docker.Cmd},
			Mounts:   mounts,
			Labels:   map[string]string{"type": "command"},
		})
	}

	// Each output parameter delocalizes a file.
	for _, parameter := range input.EphemeralPipeline.OutputParameters {
		value := parameterValue(parameter, arguments.Outputs)
		if parameter.LocalCopy != nil {
			mount := &v2.Mount{
				Disk: parameter.LocalCopy.Disk,
				Path: mountPoints[parameter.LocalCopy.Disk],
			}
			environment[parameter.Name] = path.Join(mount.Path, parameter.LocalCopy.Path)
			action := gsutil("cp", environment[parameter.Name], value)
			action.Mounts = []*v2.Mount{mount}
			action.Labels = map[string]string{"type": "output"}
			actions = append(actions, action)
		}
	}

	// Different information is logged in v2alpha1 so a perfect migration is
	// impossible.  This copies the combined standard output and standard error
	// of the actions to where the v1alpha2 log would have been written.
	if logging := arguments.Logging; logging != nil {
		filename := logging.GcsPath
		if !strings.HasSuffix(logging.GcsPath, ".log") {
			rand.Seed(time.Now().UTC().UnixNano())
			filename += fmt.Sprintf("/%08x.log", rand.Uint64())
		}
		action := gsutil("-q", "cp", "/google/logs/output", filename)
		action.Flags = []string{"ALWAYS_RUN"}
		action.Labels = map[string]string{"type": "logging"}
		actions = append(actions, action)
	}

	// To support VM keep alive, simply append a conditional sleep as an action.
	// Note that this by itself does not enable SSH or other debuggers, however.
	if duration := arguments.KeepVmAliveOnFailureDuration; duration != "" {
		actions = append(actions, &v2.Action{
			ImageUri: "bash",
			Commands: []string{"-c", fmt.Sprintf(`[ -z "${GOOGLE_PIPELINE_FAILED}" ] || sleep %s`, duration)},
			Flags:    []string{"ALWAYS_RUN"},
			Labels:   map[string]string{"type": "vm-keep-alive"},
		})
	}

	// Build the service account.  The v1alpha2 API always requested GCS access.
	serviceAccount := &v2.ServiceAccount{
		Scopes: []string{"https://www.googleapis.com/auth/devstorage.read_write"},
	}
	if account := arguments.ServiceAccount; account != nil {
		serviceAccount.Email = account.Email
		serviceAccount.Scopes = append(serviceAccount.Scopes, account.Scopes...)
	}

	var projectID string
	if v := arguments.ProjectId; v != "" {
		projectID = v
	} else if v := input.EphemeralPipeline.ProjectId; v != "" {
		projectID = v
	}

	// Name and description aren't supported in v2alpha1.  Instead, try to
	// migrate them to operation labels.  It's not safe to apply them to the VM,
	// though, since VM labels have significant restrictions.
	labels := copyMap(arguments.Labels)
	maybeMigrateToLabel(input.EphemeralPipeline.Name, "name", labels)
	maybeMigrateToLabel(input.EphemeralPipeline.Description, "description", labels)

	output := &v2.RunPipelineRequest{
		Pipeline: &v2.Pipeline{
			Actions:     actions,
			Environment: environment,
			Resources: &v2.Resources{
				ProjectId: projectID,
				Zones:     resources.Zones,
				VirtualMachine: &v2.VirtualMachine{
					MachineType:    getMachineType(resources),
					Disks:          getDisks(resources),
					Preemptible:    resources.Preemptible,
					BootDiskSizeGb: resources.BootDiskSizeGb,
					Network: &v2.Network{
						UsePrivateAddress: resources.NoAddress,
					},
					ServiceAccount: serviceAccount,
					Labels:         arguments.Labels,
				},
			},
		},
		Labels: labels,
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(&output); err != nil {
		exitf("Failed to write output pipeline: %v, err")
	}
}

func mergeResources(defaults, runtime *v1.PipelineResources) *v1.PipelineResources {
	if runtime != nil {
		if n := runtime.BootDiskSizeGb; n > 0 {
			defaults.BootDiskSizeGb = n
		}
		if n := runtime.MinimumCpuCores; n > 0 {
			defaults.MinimumCpuCores = n
		}
		if n := runtime.MinimumRamGb; n > 0 {
			defaults.MinimumRamGb = n
		}
		if zones := runtime.Zones; len(zones) > 0 {
			defaults.Zones = zones
		}
		if runtime.Preemptible {
			defaults.Preemptible = true
		}
		if runtime.NoAddress {
			defaults.NoAddress = true
		}
		// Most disk properties (including the mount point) cannot be changed.
		for _, runtimeDisk := range runtime.Disks {
			for _, defaultDisk := range defaults.Disks {
				if defaultDisk.Name == runtimeDisk.Name {
					if n := runtimeDisk.SizeGb; n > 0 {
						defaultDisk.SizeGb = n
					}
					break
				}
			}
		}
	}
	return defaults
}

func getMachineType(resources *v1.PipelineResources) string {
	return fmt.Sprintf("custom-%d-%d", resources.MinimumCpuCores, int64(resources.MinimumRamGb*1024))
}

func getDisks(resources *v1.PipelineResources) []*v2.Disk {
	var disks []*v2.Disk
	for _, disk := range resources.Disks {
		disks = append(disks, &v2.Disk{
			Name:   disk.Name,
			Type:   getDiskType(disk.Type),
			SizeGb: disk.SizeGb,
		})
	}
	return disks
}

func getDiskType(input string) string {
	switch input {
	case "PERSISTENT_SSD":
		return "pd-ssd"
	case "LOCAL_SSD":
		return "local-ssd"
	default:
		return "pd-standard"
	}
}

func parameterValue(parameter *v1.PipelineParameter, values map[string]string) string {
	if value, ok := values[parameter.Name]; ok {
		return value
	}
	return parameter.DefaultValue
}

func gsutil(arguments ...string) *v2.Action {
	return &v2.Action{
		ImageUri: cloudSDKImage,
		Commands: append([]string{"gsutil"}, arguments...),
	}
}

func maybeMigrateToLabel(value, name string, labels map[string]string) {
	if value != "" {
		if _, exists := labels[name]; exists {
			warnf("Discarding %q property since a label with the same name exists", name)
		} else {
			labels[name] = value
		}
	}
}

func copyMap(input map[string]string) map[string]string {
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func warnf(format string, arguments ...interface{}) {
	fmt.Fprintf(os.Stderr, "WARNING: "+format+"\n", arguments...)
}

func exitf(format string, arguments ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", arguments...)
	os.Exit(1)
}
