#!/bin/bash

set -ex

. `dirname "$0"`/../spec/shared/shlib/distro.sh
. `dirname "$0"`/../spec/shared/shlib/set_env.sh
. `dirname "$0"`/../spec/shared/shlib/server.sh
. `dirname "$0"`/functions.sh

set_env_vars
set_env_python
set_env_ruby

sudo apt-get -y install libyaml-dev cmake

bundle_install

echo "Running specs"
export MONGO_RUBY_DRIVER_CRYPT_SHARED_LIB_PATH=${CRYPT_SHARED_LIB_PATH}
bundle exec rake spec:prepare
bundle exec rspec spec/integration/client_side_encryption/on_demand_gcp_credentials_spec.rb

exit ${test_status}
