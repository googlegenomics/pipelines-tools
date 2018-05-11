#!/bin/sh
#
# Copyright 2018 Google LLC
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

set -eu

fail() {
  echo $@ >&2
  exit 1
}

if test "${1:-}" = "wait"; then
  target="${2:-}"
  timeout="${3:-10}"
  test -n "${target}" || fail "No mount point specified"

  delay=1
  elapsed=0
  until mountpoint -q "${target}"; do
    test "${elapsed}" -lt "${timeout}" || fail "Timed out waiting on ${target}"
    sleep "${delay}"
    elapsed=$((elapsed+delay))
  done
else
  /go/bin/gcsfuse $@
fi
