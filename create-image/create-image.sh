#!/bin/bash
set -o errexit -o pipefail -o nounset

if [[ $# != 3 ]]; then
  echo "Usage: create-image.sh <gs://source/bucket> <disk-size> <image-name>"
  exit 1
fi

if [[ "${1:0:5}" != "gs://" ]]; then
  echo "Source must be a Google Storage bucket"
  exit 1
fi

if [[ "${2:(-2)}" != "GB" ]] && [[ "${2:(-2)}" != "TB" ]]; then
  echo "Disk size should be XXGB or XXTB"
  exit 1
fi

echo "Files to copy:"
gsutil ls "${1}"

ZONE=us-central1-a
SCRIPT=$(mktemp -t "run-script-XXXXX")

echo "VM startup script: ${SCRIPT}"
DATA=/mnt/disks/data
cat <<EOF >${SCRIPT}
set -e -x
sudo mkfs.ext4 -m 0 -F -E lazy_itable_init=0,lazy_journal_init=0,discard \
  /dev/sdb
sudo mkdir -p "${DATA}"
sudo mount -o discard,defaults /dev/sdb "${DATA}"
sudo chmod a+w "${DATA}"

docker run -v "${DATA}":"${DATA}" \
  google/cloud-sdk:slim gsutil cp -r "${1}" "${DATA}"

/sbin/poweroff
EOF

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi
. "$DIR/run-script.sh"

run_script "${SCRIPT}" "$2" "${ZONE}" "$3"
rm "${SCRIPT}"

