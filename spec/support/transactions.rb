# Copyright (C) 2014-2019 MongoDB, Inc.
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

require 'support/transactions/context'
require 'support/transactions/operation'
require 'support/transactions/spec'
require 'support/transactions/test'

def define_transactions_spec_tests(test_paths)

  test_paths.each do |file|

    spec = Mongo::Transactions::Spec.new(file)

    context(spec.description) do
      define_spec_tests_with_requirements(spec) do |req|
        spec.tests.each do |test|

          context(test.description) do

            if test.skip_reason
              before(:all) do
                skip test.skip_reason
              end
            end

            before(:all) do
              if req.satisfied?
                test.setup_test
              end
            end

            after(:all) do
              if req.satisfied?
                test.teardown_test
              end
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
