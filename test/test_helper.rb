# Copyright (C) 2009-2013 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# NOTE: on ruby <1.9 you need to run individual tests with 'bundle exec'

unless RUBY_VERSION < '1.9' || ENV.key?('JENKINS_CI')
  require 'simplecov'
  require 'coveralls'

  SimpleCov.formatter = SimpleCov::Formatter::MultiFormatter[
    SimpleCov::Formatter::HTMLFormatter,
    Coveralls::SimpleCov::Formatter
  ]

  SimpleCov.start do
    add_group 'Driver', 'lib/mongo'
    add_group 'BSON', 'lib/bson'

    add_filter 'tasks'
    add_filter 'test'
    add_filter 'bin'
  end
end

# required for at_exit, at_start hooks
require 'test-unit'

require 'test/unit'
require 'shoulda'
require 'mocha/setup'

# cluster manager
require 'tools/mongo_config'

# For kerberos testing.
begin
  require 'mongo_kerberos'
rescue LoadError; end

# test helpers
require 'helpers/general'
require 'helpers/test_unit'

# optional development and debug utilities
begin
  require 'pry-rescue'
  require 'pry-nav'
rescue LoadError
  # failed to load, skipping pry
end
