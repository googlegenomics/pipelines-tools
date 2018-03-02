# Google Genomics Pipelines Tools
[![Build Status](https://travis-ci.org/googlegenomics/pipelines-tools.svg?branch=master)](https://travis-ci.org/googlegenomics/pipelines-tools)

This repository contains various tools that are useful when running pipelines
with the [Google Genomics API][1]. 

# Building the tools

In order to build the tools, you will need the [Go][2] tool
chain (at least version 1.8).

Once Go is installed, you can build the tools by running:

```
$ go get github.com/googlegenomics/pipelines-tools/...
```

This will produce binaries in $GOPATH/bin for each tool.

# Usage

## The `pipelines` tool

This tool provides support for running, cancelling and inspecting pipelines.

As a simple example, to run a pipeline that prints 'hello world':

```
$ cat <<EOF > hello.script
echo "hello world"
EOF
$ pipelines --project=my-project run --script=hello.script --output=gs://my-bucket/logs --wait
```

After the pipeline finishes, you can inspect the output using `gsutil`:

```
$ gsutil cat gs://my-bucket/logs/output
```

The script file format is described in the [source code for the command][3].

## The `migrate-pipeline` tool

This tool takes a JSON encoded v1alpha2 run pipeline request and attempts to
emit a v2alpha1 request that replicates the same behaviour.

For example, given a file `v1.jsonpb` that has a request containing a v1alpha2
ephemeral pipeline and arguments, running:

```
$ migrate-pipeline < v1.jsonpb
```

will produce a v2alpha1 request that performs the same action on standard
output.

# Support

Please report problems using the issue tracker.

[1]: https://cloud.google.com/genomics
[2]: https://golang.org/
[3]: https://github.com/googlegenomics/pipelines-tools/blob/master/pipelines/internal/commands/run/run.go#L18
