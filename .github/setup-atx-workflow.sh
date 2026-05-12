#!/bin/bash
# This script moves the ATX transform workflow to the correct location.
# GitHub API restrictions on fork repositories prevent direct creation of
# files in .github/workflows/ via the Contents API.
#
# Run this script after cloning the branch locally:
#   chmod +x .github/setup-atx-workflow.sh
#   ./.github/setup-atx-workflow.sh
#   git add .github/workflows/atx-transform.yml
#   git rm .github/atx-transform.yml .github/setup-atx-workflow.sh
#   git commit -m "Move ATX workflow to correct location"
#   git push

set -e

mkdir -p .github/workflows
mv .github/atx-transform.yml .github/workflows/atx-transform.yml
echo "Moved .github/atx-transform.yml -> .github/workflows/atx-transform.yml"
echo "You can now remove this setup script: rm .github/setup-atx-workflow.sh"
