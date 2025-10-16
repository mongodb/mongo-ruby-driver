#!/bin/bash

set -ex

. `dirname "$0"`/../spec/shared/shlib/distro.sh
. `dirname "$0"`/../spec/shared/shlib/set_env.sh
. `dirname "$0"`/functions.sh

set_env_vars
set_env_ruby

bundle_install

if [ -f "${PROJECT_DIRECTORY}/secrets-export.sh" ]; then
    source ${PROJECT_DIRECTORY}/secrets-export.sh
fi

echo "Running specs"

export ATLAS_TESTING=1

bundle exec rspec spec/atlas --format 'Rfc::Riff' --format RspecJunitFormatter --out rspec.xml
