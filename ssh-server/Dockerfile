# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM golang:1.10

WORKDIR /go/src/github.com/googlegenomics/pipelines-tools/ssh-server
COPY . .

RUN go get -d -v ./...
RUN go install -v ./...

EXPOSE 22

# By default, the server listens on port 22
ENTRYPOINT ["ssh-server"]

RUN apt-get update
RUN apt-get -y install openssh-client
RUN ssh-keygen -q -t rsa -N '' -f id_rsa
