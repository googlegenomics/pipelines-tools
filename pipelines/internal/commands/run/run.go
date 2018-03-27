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

// Package run provides a sub tool for running pipelines.
package run

// This tool takes an input file that describes a pipeline and runs it using
// the Pipelines API.
//
// The input file can be:
// - a raw JSON encoded API request
// - a JSON encoded array of action objects
// - a script file (whose format is described below)
//
// The input filename must be specified as a single positional argument (though
// it can appear before or after other options).  If the input filename is '-',
// the tool reads from standard input.
//
// If a raw request is given as an input the tool does not do any of the
// additional processing described below.
//
// The script file format consists of a series of command lines.  Each line is
// executed using the 'bash' container and must succeed before the next command
// is executed.  This behaviour can be modified using a '&' at the end of the
// line which will cause the command to run in the background.  Additionally,
// flags and options can be specified after a '#' character to control what
// image is used or to apply other action flags.  A trailing '\' can be used to
// break up a long line.
//
// Files from GCS can be specified as inputs to the pipeline using the --inputs
// flag.  These files will be copied onto the VM.  The names of the localized
// files are exposed via the environment variables $INPUT0 to $INPUTN.  Note
// that the input files are placed on a read-only disk.
//
// Similarly, destinations in GCS may be specified with the --outputs flag.
// Each output file will be exposed by via the environment variables $OUTPUT0
// to $OUTPUTN.
//
// As a convenience, the tool will automatically use the google/cloud-sdk image
// whenever the command line starts with gsutil or gcloud, and will
// automatically include the cloud-platform API scope whenever the cloud-sdk
// container is used.
//
// Any 'export' commands are interpreted prior to execution of the pipeline.
// The specified value is injected into the environment for all subsequent
// commands.
//
// Variable references of the form ${VARIABLE} or $VARIABLE are replaced using
// values from the host environment (unless the value has been overwritten by a
// previous export command).
//
// If any command references '/tmp' the attached persistent disk is mounted
// there, read-write.
//
// If the --output flag is specified, an action is appended that copies the
// combined pipeline output to the specified GCS path.
//
// The --dry-run flag can be used to see what pipeline would be produced
// without executing it.
//
// Example 1: Simple 'hello world' script
//
//    echo "Hello World!"
//
// Example 2: Simple 'hello world' actions file
//
//    [
//      {
//        "imageUri": "bash",
//        "commands": [
//          "-c", "echo \"Hello World!\""
//        ]
//      }
//    ]
//
// Example 3: Background action script
//
//    while true; do echo "Hello background world!"; sleep 1; done &
//    sleep 5
//    echo "Hello foreground world!"
//
// Example 4: BAM indexing script
//
//    export BAM=NA12892_S1.bam
//    export BAI=${BAM}.bai
//    gsutil cp gs://my-bucket/${BAM} /tmp
//    index /tmp/${BAM} /tmp/${BAI} # image=gcr.io/genomics-tools/samtools
//    gsutil cp /tmp/${BAI} gs://my-bucket/
//
// Example 5: Script to SHA1 sum a file (using --inputs and --outputs).
//
//    sha1sum ${INPUT0} > ${OUTPUT0}
//
// Example 6: Using netcat to dump logs for the running pipeline
//
//    nc -n -l -p 1234 -e tail -f /google/logs/output & # ports=1234:22
//
import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/googlegenomics/pipelines-tools/pipelines/internal/commands/watch"
	"github.com/googlegenomics/pipelines-tools/pipelines/internal/common"
	"golang.org/x/oauth2/google"
	compute "google.golang.org/api/compute/v1"
	genomics "google.golang.org/api/genomics/v2alpha1"
	"google.golang.org/api/googleapi"
)

var (
	flags = flag.NewFlagSet("", flag.ExitOnError)

	basePath       = flags.String("base-path", "", "optional API service base path")
	name           = flags.String("name", "", "optional name applied as a label")
	scopes         = flags.String("scopes", "", "comma separated list of additional API scopes")
	zones          = flags.String("zones", "us-east1-d", "comma separated list of zone names or prefixes (e.g. us-*)")
	output         = flags.String("output", "", "GCS path to write output to")
	dryRun         = flags.Bool("dry-run", false, "don't run, just show pipeline")
	wait           = flags.Bool("wait", true, "wait for the pipeline to finish")
	machineType    = flags.String("machine-type", "n1-standard-1", "machine type to create")
	preemptible    = flags.Bool("preemptible", true, "use a preemptible VM")
	inputs         = flags.String("inputs", "", "comma separated list of GCS objects to localize to the VM")
	outputs        = flags.String("outputs", "", "comma separated list of GCS objects to delocalize from the VM")
	diskSizeGb     = flags.Int("disk-size", 500, "the attached disk size (in GB)")
	bootDiskSizeGb = flags.Int("boot-disk-size", 0, "if non-zero, specifies the boot disk size (in GB)")
	privateAddress = flags.Bool("private-address", false, "use a private IP address")
	cloudSDKImage  = flags.String("cloud_sdk_image", "google/cloud-sdk:alpine", "the cloud SDK image to use")
	timeout        = flags.Duration("timeout", 0, "how long to wait before the operation is abandoned")
)

const (
	inputRoot  = "/mnt/input"
	outputRoot = "/mnt/output"
	diskName   = "shared"
)

func Invoke(ctx context.Context, service *genomics.Service, project string, arguments []string) error {
	filenames := common.ParseFlags(flags, arguments)
	if len(filenames) != 1 {
		return errors.New("a single input file is required")
	}

	filename := filenames[0]

	req, err := buildRequest(filename, project)
	if err != nil {
		return fmt.Errorf("building request: %v", err)
	}

	encoded, err := json.MarshalIndent(req, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding request: %v", err)
	}
	fmt.Printf("%s\n", encoded)

	if *dryRun {
		return nil
	}

	lro, err := service.Pipelines.Run(req).Context(ctx).Do()
	if err != nil {
		if err, ok := err.(*googleapi.Error); ok {
			return fmt.Errorf("starting pipeline: %q: %q", err.Message, err.Body)
		}
		return fmt.Errorf("starting pipeline: %v", err)
	}

	cancelOnInterruptOrTimeout(ctx, service, lro.Name, *timeout)

	fmt.Printf("Pipeline running as %q\n", lro.Name)
	if *output != "" {
		fmt.Printf("Output will be written to %q\n", *output)
	}

	if !*wait {
		return nil
	}

	return watch.Invoke(ctx, service, project, []string{lro.Name})
}

func parseJSON(filename string, v interface{}) error {
	f, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("opening file: %v", err)
	}
	defer f.Close()

	return json.NewDecoder(f).Decode(v)
}

func buildRequest(filename, project string) (*genomics.RunPipelineRequest, error) {
	var req genomics.RunPipelineRequest
	if err := parseJSON(filename, &req); err == nil {
		return &req, nil
	}

	labels := make(map[string]string)
	if *name != "" {
		labels["name"] = *name
	}

	environment := make(map[string]string)
	filenames := make(map[string]int)

	var localizers, delocalizers []*genomics.Action
	for i, input := range listOf(*inputs) {
		filenames[path.Base(input)]++
		filename := filepath.Join(inputRoot, path.Base(input))
		localizers = append(localizers, &genomics.Action{
			ImageUri: *cloudSDKImage,
			Commands: []string{"gsutil", "-q", "cp", input, filename},
			Mounts:   []*genomics.Mount{{Disk: diskName, Path: inputRoot}},
		})
		environment[fmt.Sprintf("INPUT%d", i)] = filename
	}
	for i, output := range listOf(*outputs) {
		filenames[path.Base(output)]++
		filename := filepath.Join(outputRoot, path.Base(output))
		delocalizers = append(delocalizers, &genomics.Action{
			ImageUri: *cloudSDKImage,
			Commands: []string{"gsutil", "-q", "cp", filename, output},
			Flags:    []string{"ALWAYS_RUN"},
			Mounts:   []*genomics.Mount{{Disk: diskName, Path: outputRoot, ReadOnly: true}},
		})
		environment[fmt.Sprintf("OUTPUT%d", i)] = filename
	}
	for filename, count := range filenames {
		if count > 1 {
			return nil, fmt.Errorf("duplicate filename %q is not supported", filename)
		}
	}

	if *output != "" {
		delocalizers = append(delocalizers, &genomics.Action{
			ImageUri: *cloudSDKImage,
			Commands: []string{"gsutil", "-q", "cp", "/google/logs/output", *output},
			Flags:    []string{"ALWAYS_RUN"},
		})
	}

	var actions []*genomics.Action
	if err := parseJSON(filename, &actions); err != nil {
		v, err := parseScript(filename, environment)
		if err != nil {
			return nil, fmt.Errorf("creating pipeline from script: %v", err)
		}
		actions = v
	}

	zones, err := expandZones(project, listOf(*zones))
	if err != nil {
		return nil, fmt.Errorf("expanding zones: %v", err)
	}

	pipeline := &genomics.Pipeline{
		Resources: &genomics.Resources{
			ProjectId: project,
			Zones:     zones,
			VirtualMachine: &genomics.VirtualMachine{
				MachineType: *machineType,
				Preemptible: *preemptible,
				Network: &genomics.Network{
					UsePrivateAddress: *privateAddress,
				},
				ServiceAccount: &genomics.ServiceAccount{Scopes: listOf(*scopes)},
			},
		},
		Actions:     append(localizers, append(actions, delocalizers...)...),
		Environment: environment,
	}

	addRequiredDisks(pipeline)
	addRequiredScopes(pipeline)

	return &genomics.RunPipelineRequest{Pipeline: pipeline, Labels: labels}, nil
}

func parseScript(filename string, globalEnv map[string]string) ([]*genomics.Action, error) {
	var scanner *bufio.Scanner
	if filename == "-" {
		scanner = bufio.NewScanner(os.Stdin)
	} else {
		f, err := os.Open(filename)
		if err != nil {
			return nil, fmt.Errorf("opening script: %v", err)
		}
		defer f.Close()

		scanner = bufio.NewScanner(f)
	}

	var line int
	var buffer strings.Builder
	var actions []*genomics.Action
	localEnv := make(map[string]string)
	for scanner.Scan() {
		text := scanner.Text()
		line++

		if strings.HasSuffix(text, "\\") {
			buffer.WriteString(text[:len(text)-1])
			continue
		}

		buffer.WriteString(text)

		action, err := parse(buffer.String(), localEnv, globalEnv)
		if err != nil {
			return nil, fmt.Errorf("line %d: %v", line, err)
		}
		if action != nil {
			actions = append(actions, action)
		}

		buffer.Reset()
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading script: %v", err)
	}
	return actions, nil
}

func parse(line string, localEnv, globalEnv map[string]string) (*genomics.Action, error) {
	var (
		commands []string
		flags    []string
		options  = make(map[string]string)
	)

	if n := strings.Index(line, "#"); n >= 0 {
		for _, option := range strings.Fields(strings.TrimSpace(line[n+1:])) {
			if n := strings.Index(option, "="); n >= 0 {
				options[option[:n]] = option[n+1:]
			} else {
				flags = append(flags, strings.ToUpper(option))
			}
		}
		line = line[:n]
	}

	var missing string
	line = os.Expand(line, func(name string) string {
		if value, ok := localEnv[name]; ok {
			return value
		}
		if value, ok := globalEnv[name]; ok {
			return value
		}
		if value, ok := os.LookupEnv(name); ok {
			return value
		}
		if isRuntimeVariable(name) {
			return fmt.Sprintf("${%s}", name)
		}
		missing = name
		return ""
	})

	if missing != "" {
		return nil, fmt.Errorf("missing value for variable %q", missing)
	}

	commands = strings.Fields(strings.TrimSpace(line))
	if len(commands) == 0 {
		return nil, nil
	}

	if commands[len(commands)-1] == "&" {
		flags = append(flags, "RUN_IN_BACKGROUND")
		commands = commands[:len(commands)-1]
	}

	if commands[0] == "export" {
		fields := strings.SplitN(strings.Join(commands[1:], " "), "=", 2)
		if len(fields) != 2 {
			return nil, fmt.Errorf("missing assignment in export command: %q", line)
		}

		localEnv[fields[0]] = fields[1]
		return nil, nil
	}

	image := detectImage(commands, options)
	mounts := detectMounts(commands)
	if image == "bash" {
		commands = []string{"-c", strings.Join(commands, " ")}
	}

	action := &genomics.Action{
		ImageUri:    image,
		Commands:    commands,
		Flags:       flags,
		Environment: localEnv,
		Mounts:      mounts,
	}

	if v, ok := options["ports"]; ok {
		ports, err := parsePorts(v)
		if err != nil {
			return nil, fmt.Errorf("parsing ports: %v", err)
		}
		action.PortMappings = ports
	}
	return action, nil
}

func detectImage(command []string, options map[string]string) string {
	if image, ok := options["image"]; ok {
		return image
	}
	if isCloudCommand(command[0]) {
		return *cloudSDKImage
	}
	return "bash"
}

func detectMounts(commands []string) []*genomics.Mount {
	var mounts []*genomics.Mount
	if findReference("/tmp", commands) {
		mounts = append(mounts, &genomics.Mount{Disk: diskName, Path: "/tmp"})
	}
	if findReference(outputRoot, commands) {
		mounts = append(mounts, &genomics.Mount{Disk: diskName, Path: outputRoot})
	}
	if findReference(inputRoot, commands) {
		mounts = append(mounts, &genomics.Mount{Disk: diskName, Path: inputRoot, ReadOnly: true})
	}
	return mounts
}

func findReference(root string, commands []string) bool {
	for _, command := range commands {
		if strings.HasPrefix(command, root) {
			return true
		}
	}
	return false
}

func addRequiredDisks(pipeline *genomics.Pipeline) {
	disks := make(map[string]bool)
	for _, action := range pipeline.Actions {
		for _, mount := range action.Mounts {
			disks[mount.Disk] = true
		}
	}

	vm := pipeline.Resources.VirtualMachine
	for name := range disks {
		vm.Disks = append(vm.Disks, &genomics.Disk{
			Name:   name,
			SizeGb: int64(*diskSizeGb),
		})
	}
	if *bootDiskSizeGb > 0 {
		vm.BootDiskSizeGb = int64(*bootDiskSizeGb)
	}
}

func addRequiredScopes(pipeline *genomics.Pipeline) {
	scopes := &pipeline.Resources.VirtualMachine.ServiceAccount.Scopes
	for _, action := range pipeline.Actions {
		if strings.HasPrefix(action.ImageUri, "google/cloud-sdk") || isCloudCommand(action.Commands[0]) {
			*scopes = append(*scopes, "https://www.googleapis.com/auth/devstorage.read_write")
			return
		}
	}
}

func isCloudCommand(command string) bool {
	return command == "gsutil" || command == "gcloud"
}

func listOf(input string) []string {
	if input == "" {
		return nil
	}
	return strings.Split(input, ",")
}

func expandZones(project string, input []string) ([]string, error) {
	var zones, prefixes []string
	for _, zone := range input {
		if strings.HasSuffix(zone, "*") {
			prefixes = append(prefixes, zone[:len(zone)-1])
		} else {
			zones = append(zones, zone)
		}
	}
	if len(prefixes) > 0 {
		allZones, err := listZones(project)
		if err != nil {
			return nil, err
		}
		for _, zone := range allZones {
			for _, prefix := range prefixes {
				if strings.HasPrefix(zone, prefix) {
					zones = append(zones, zone)
					break
				}
			}
		}
	}
	return zones, nil
}

func listZones(project string) ([]string, error) {
	client, err := google.DefaultClient(context.Background(), compute.ComputeScope)
	if err != nil {
		return nil, fmt.Errorf("creating compute client: %v", err)
	}
	service, err := compute.New(client)
	if err != nil {
		return nil, fmt.Errorf("creating compute service: %v", err)
	}

	resp, err := service.Zones.List(project).Do()
	if err != nil {
		return nil, fmt.Errorf("listing zones: %v", err)
	}

	var zones []string
	for _, zone := range resp.Items {
		zones = append(zones, zone.Name)
	}
	return zones, nil
}

func parsePorts(input string) (map[string]int64, error) {
	ports := make(map[string]int64)
	for _, pair := range strings.Split(input, ";") {
		i := strings.Index(pair, ":")
		if i < 1 {
			return nil, fmt.Errorf("invalid port mapping %q", pair)
		}
		n, err := strconv.ParseInt(pair[i+1:], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parsing host port: %v", err)
		}
		ports[pair[:i]] = n
	}
	return ports, nil
}

func isRuntimeVariable(name string) bool {
	return name == "GOOGLE_PIPELINE_FAILED" || name == "GOOGLE_LAST_EXIT_STATUS"
}

func cancelOnInterruptOrTimeout(ctx context.Context, service *genomics.Service, name string, timeout time.Duration) {
	var ticker <-chan time.Time
	if timeout > 0 {
		ticker = time.After(timeout)
	}

	abort := make(chan os.Signal, 1)
	signal.Notify(abort, os.Interrupt)
	go func() {
		select {
		case <-abort:
		case <-ticker:
			fmt.Println("User specified timeout has been reached")
		}
		fmt.Println("Cancelling operation...")
		req := &genomics.CancelOperationRequest{}
		if _, err := service.Projects.Operations.Cancel(name, req).Context(ctx).Do(); err != nil {
			fmt.Printf("Failed to cancel operation: %v\n", err)
		}
	}()
}
