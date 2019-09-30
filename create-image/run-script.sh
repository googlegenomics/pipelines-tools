#!/bin/bash

#######################################
# Run a script on a VM and create a disk image.
# Arguments:
#   Script to run, a path.
#   Disk size to pass to GCE, a string.
#   Zone to run in, a string.
#   Output image name, a string.
# #######################################
function run_script() {
  local NAME=$(echo "run-script-${RANDOM}")
  local DISK_NAME="${NAME}-disk"
  local SCRIPT="$1"
  local SIZE="$2"
  local ZONE="$3"
  local IMAGE="$4"

  echo "Creating instance..."
  gcloud compute instances create "${NAME}" \
    --zone "${ZONE}" --image-family cos-stable --image-project cos-cloud \
    --create-disk "name=${DISK_NAME},size=${SIZE},auto-delete=yes" \
    --metadata-from-file startup-script="${SCRIPT}"

  echo -n "Waiting for VM..."
  while true; do
    local STATUS="$(gcloud compute instances describe ${NAME} \
      --zone ${ZONE} --format='get(status)')"
    if [[ "${STATUS}" != "RUNNING" ]]; then
      break
    fi
    sleep 10
    echo -n "."
  done
  echo "Done"

  echo "Creating image..."
  gcloud compute images create "${IMAGE}" \
    --source-disk "${DISK_NAME}" --source-disk-zone "${ZONE}" --force

  echo "Deleting VM..."
  gcloud compute instances delete "${NAME}" -q --zone "${ZONE}"
}
