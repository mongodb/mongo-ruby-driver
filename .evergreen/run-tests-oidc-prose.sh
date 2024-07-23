#!/bin/bash

set -ex

ENVIRONMENT=${ENVIRONMENT:-"test"}

. `dirname "$0"`/../spec/shared/shlib/distro.sh
. `dirname "$0"`/../spec/shared/shlib/set_env.sh
. `dirname "$0"`/functions.sh

set_env_vars
set_env_python
set_env_ruby

sudo apt-get -y install libyaml-dev cmake

bundle_install
bundle exec rspec -fd spec/integration/oidc/${ENVIRONMENT}_machine_auth_flow_prose_spec.rb

test_status=$?

kill_jruby

exit ${test_status}
