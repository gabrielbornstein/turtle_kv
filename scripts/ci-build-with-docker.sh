#!/bin/bash -x
#
set -Eeuo pipefail

env

SCRIPT_DIR=$(realpath $(dirname "$0"))
PROJECT_DIR=$(realpath $(dirname "${SCRIPT_DIR}"))

. "${SCRIPT_DIR}/install-cor.sh"

# Run the ci-build.sh script using docker.
#
ROOT_IMAGE=registry.gitlab.com/batteriesincluded/batt-docker/batteries-debian12-build-tools:0.5.0
USER_IMAGE=$(cor docker user-image ${ROOT_IMAGE})

if [ -f "${GITHUB_ENV:-noSuchFile}" ]; then
    ENV_FILE_FLAGS="--env-file ${GITHUB_ENV}"
    cat "${GITHUB_ENV}"
fi

docker run \
       --ulimit memlock=-1:-1 \
       --cap-add SYS_ADMIN \
       --privileged \
       --network host \
       ${ENV_FILE_FLAGS:-} \
       --volume "${HOME}/ci_conan_hosts:/etc/hosts:ro" \
       --volume "${PROJECT_DIR}:${PROJECT_DIR}" \
       --workdir "${PROJECT_DIR}" \
       ${USER_IMAGE} \
       "${BUILD_COMMAND:-${SCRIPT_DIR}/ci-build.sh}"
