# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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

require 'runners/transactions/operation'
require 'runners/transactions/spec'
require 'runners/transactions/test'

def define_transactions_spec_tests(test_paths, expectations_bson_types: true)
  config_override :validate_update_replace, true

  test_paths.each do |file|

    spec = Mongo::Transactions::Spec.new(file)

    context(spec.description) do

      define_spec_tests_with_requirements(spec) do |req|

        spec.tests(expectations_bson_types: expectations_bson_types).each do |test|

          context(test.description) do

            before(:all) do
              if ClusterConfig.instance.topology == :sharded
                if test.multiple_mongoses? && SpecConfig.instance.addresses.length == 1
                  skip "Test requires multiple mongoses"
                elsif !test.multiple_mongoses? && SpecConfig.instance.addresses.length > 1
                  # Many transaction spec tests that do not specifically deal with
                  # sharded transactions fail when run against a multi-mongos cluster
                  skip "Test does not specify multiple mongoses"
                end
              end
            end

            if test.skip_reason
              before(:all) do
                skip test.skip_reason
              end
            end

            unless req.satisfied?
              before(:all) do
                skip "Requirements not satisfied"
              end
            end

            before(:all) do
              test.setup_test
            end

            after(:all) do
              test.teardown_test
            end

            let(:results) do
              $tx_spec_results_cache ||= {}
              $tx_spec_results_cache[test.object_id] ||= test.run
            end

            let(:verifier) { Mongo::CRUD::Verifier.new(test) }

            it 'returns the correct results' do
              verifier.verify_operation_result(test.expected_results, results[:results])
            end

            if test.outcome && test.outcome.collection_data?
              it 'has the correct data in the collection' do
                results
                verifier.verify_collection_data(
                  test.outcome.collection_data,
                  results[:contents])
              end
            end

            if test.expectations
              it 'has the correct number of command_started events' do
                verifier.verify_command_started_event_count(
                  test.expectations, results[:events])
              end

              test.expectations.each_with_index do |expectation, i|
                it "has the correct command_started event #{i}" do
                  verifier.verify_command_started_event(
                    test.expectations, results[:events], i)
                end
              end
            end
          end
        end
      end
    end
  end
end
