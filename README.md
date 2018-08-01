# Google Genomics Pipelines Tools
[![Build Status](https://travis-ci.org/googlegenomics/pipelines-tools.svg?branch=master)](https://travis-ci.org/googlegenomics/pipelines-tools)

This repository contains various tools that are useful when running pipelines
with the [Google Genomics API][1]. 

# Quick Start Using Cloud Shell

1. Enable the Genomics API and the Compute Engine API in a new or existing
Google Cloud project.
2. Start a [Cloud Shell][cloud-shell] inside your project.
3. Inside the Cloud Shell, run the command

        go get github.com/googlegenomics/pipelines-tools/...

   This command downloads and installs the pipelines tools.  Note that to build
   these tools outside the Cloud Shell you will need the [Go][2] tool chain.

4. Make a bucket on GCS to store the output from the pipeline:

        export BUCKET=gs://${GOOGLE_CLOUD_PROJECT}-pipelines
        gsutil mb ${BUCKET}

5. Put some test data into the bucket:

        echo "Hello World" | gsutil cp - ${BUCKET}/input

6. Make a pipeline script that computes the SHA1 sum of a file:

        echo 'sha1sum ${INPUT0} > ${OUTPUT0}' > sha1.script

7. Run the script using the pipelines API:

        pipelines run --inputs=${BUCKET}/input --outputs=${BUCKET}/output sha1.script

8. Check the generated output file:

        gsutil cat ${BUCKET}/output

That's it: you've run your first pipeline.  For more information about the
input formats supported by the pipelines tool, check out the [source code][3].
To learn more about the Pipelines API, consult the [reference
documentation][api-reference].

# Usage

## The `pipelines` tool

This tool provides support for running, cancelling and inspecting pipelines.

As a simple example, to run a pipeline that prints 'hello world':

```
$ cat <<EOF > hello.script
echo "hello world"
EOF
$ pipelines --project=my-project run hello.script --output=gs://my-bucket/logs
```

After the pipeline finishes, you can inspect the output using `gsutil`:

```
$ gsutil cat gs://my-bucket/logs/output
```

The script file format is described in the [source code for the command][3].

### Using gcsfuse with the pipelines tool

Use `--fuse` flag to allow the `pipelines` tool to use [gcsfuse][gcs-fuse] to localize input files
instead of copying them one by one with `gsutil`.

Files other than those directly mentioned by the `--inputs` flag will be
available to container, since the entire bucket is mounted.

### SSH into the worker machine

The pipelines tool can start an ssh container in the background to allow you 
to log in using SSH and view logs in real time.

To be able to use the `--ssh` flag you have to build a ssh-server docker container. 
To do this, enter the `ssh-server` subdirectory and run: 

```
gcloud auth configure-docker
docker build -t gcr.io/${GOOGLE_CLOUD_PROJECT}/ssh-server .
docker push gcr.io/${GOOGLE_CLOUD_PROJECT}/ssh-server
```

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
[cloud-shell]: https://cloud.google.com/shell/docs/quickstart
[api-reference]: https://cloud.google.com/genomics/reference/rest/v2alpha1/pipelines/run
[gcs-fuse]: https://cloud.google.com/storage/docs/gcs-fuse
