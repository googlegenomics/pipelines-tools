#!/bin/bash
set -o errexit -o pipefail -o nounset

if [[ $# != 3 ]]; then
  echo "Usage: create-docker-image.sh <images,...> <disk-size> <image-name>"
  exit 1
fi

if [[ "${2:(-2)}" != "GB" ]] && [[ "${2:(-2)}" != "TB" ]]; then
  echo "Disk size should be XXGB or XXTB"
  exit 1
fi


ZONE=us-central1-a
SCRIPT=$(mktemp -t "run-script-XXXXX")

echo "VM startup script: ${SCRIPT}"
DATA=/mnt/disks/data
cat <<EOF >${SCRIPT}
set -o errexit -o xtrace
sudo mkfs.ext4 -m 0 -F -E lazy_itable_init=0,lazy_journal_init=0,discard \
  /dev/sdb
sudo mkdir -p "${DATA}"
sudo mount -o discard,defaults /dev/sdb "${DATA}"
sudo chmod a+w "${DATA}"

mount --bind "${DATA}" /var/lib/docker/overlay2
mkdir "${DATA}/l"

IFS=","
IMAGES="$1"
for IMAGE in \$IMAGES; do
  docker pull "\${IMAGE}"
done

/sbin/poweroff
EOF

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi
. "$DIR/run-script.sh"

run_script "${SCRIPT}" "$2" "${ZONE}" "$3"
rm "${SCRIPT}"

