#!/bin/bash
#
set -Eeuo pipefail

export PATH="${HOME}/.local/bin:${PATH}"
which cor 2>&1 >/dev/null || {
    pipx install cor-launcher --index-url https://gitlab.com/api/v4/projects/64628567/packages/pypi/simple
    cor-setup
}
