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

require 'support/gridfs'
require 'support/crud/requirement'
require 'support/crud/spec'
require 'support/crud/test'
require 'support/crud/outcome'
require 'support/crud/operation'
require 'support/crud/read'
require 'support/crud/write'
require 'support/crud/verifier'

def crud_execute_operations(spec, test, num_ops, event_subscriber, expect_error,
  client
)
  cache_key = "#{test.object_id}:#{num_ops}"
  $crud_result_cache ||= {}
  $crud_result_cache[cache_key] ||= begin
    if spec.bucket_name
      client["#{spec.bucket_name}.files"].delete_many
      client["#{spec.bucket_name}.chunks"].delete_many
    else
      client[spec.collection_name].delete_many
    end

    test.setup_test(spec, client)

    event_subscriber.clear_events!

    result = if expect_error.nil?
      res = nil
      begin
        res = test.run(spec, client, num_ops)
      rescue => e
        res = e
      end
      res
    elsif expect_error
      error = nil
      begin
        test.run(spec, client, num_ops)
      rescue => e
        error = e
      end
      error
    else
      test.run(spec, client, num_ops)
    end

    $crud_event_cache ||= {}
    # It only makes sense to assert on events if all operations succeeded,
    # but populate our cache in any event for simplicity
    $crud_event_cache[cache_key] = event_subscriber.started_events.dup

    last_op = test.operations[num_ops-1]
    if last_op.outcome && last_op.outcome.collection_data?
      verify_collection = client[last_op.verify_collection_name]
      $crud_collection_data_cache ||= {}
      $crud_collection_data_cache[cache_key] = verify_collection.find.to_a
    end

    result
  ensure
    test.clear_fail_point(client)
  end
end

def define_crud_spec_test_examples(spec, req = nil, &block)
  spec.tests.each do |test|

    context(test.description) do

      if test.description =~ /ListIndexNames/
        before do
          skip "Ruby driver does not implement list_index_names"
        end
      end

      let(:event_subscriber) do
        EventSubscriber.new
      end

      let(:verifier) { Mongo::CRUD::Verifier.new(test) }

      let(:verify_collection) { client[verify_collection_name] }

      instance_exec(spec, req, test, &block)

      test.operations.each_with_index do |operation, index|

        context "operation #{index+1}" do

          let(:result) do
            crud_execute_operations(spec, test, index+1,
              event_subscriber, operation.outcome.error?, client)
          end

          let(:verify_collection_name) do
            if operation.outcome && operation.outcome.collection_name
              operation.outcome.collection_name
            else
              spec.collection_name
            end
          end

          if operation.outcome.error?
            it 'raises an error' do
              expect(result).to be_a(Mongo::Error)
            end
          else
            tested = false

            if operation.outcome.result
              tested = true
              it 'returns the correct result' do
                result
                verifier.verify_operation_result(operation.outcome.result, result)
              end
            end

            if operation.outcome.collection_data?
              tested = true
              it 'has the correct data in the collection' do
                result
                verifier.verify_collection_data(
                  operation.outcome.collection_data,
                  verify_collection.find.to_a)
              end
            end

            unless tested
              it 'succeeds' do
                expect do
                  result
                end.not_to raise_error
              end
            end
          end
        end
      end

      if test.expectations
        let(:result) do
          crud_execute_operations(spec, test, test.operations.length,
            event_subscriber, nil, client)
        end

        let(:actual_events) do
          result
          Utils.yamlify_command_events($crud_event_cache["#{test.object_id}:#{test.operations.length}"])
        end

        it 'has the correct number of command_started events' do
          verifier.verify_command_started_event_count(test.expectations, actual_events)
        end

        test.expectations.each_with_index do |expectation, i|
          it "has the correct command_started event #{i+1}" do
            verifier.verify_command_started_event(
              test.expectations, actual_events, i)
          end
        end
      end
    end
  end
end

def define_spec_tests_with_requirements(spec, &block)
  if spec.requirements
    # This block defines the same set of examples multiple times,
    # once for each requirement specified in the YAML files.
    # This allows detecting when any of the configurations is
    # not tested by CI.
    spec.requirements.each do |req|
      context(req.description) do
        if req.min_server_version
          min_server_fcv req.short_min_server_version
        end
        if req.max_server_version
          max_server_version req.short_max_server_version
        end
        if req.topologies
          require_topology *req.topologies
        end

        instance_exec(req, &block)
      end
    end
  else
    yield
  end
end

def define_crud_spec_tests(test_paths, spec_cls = Mongo::CRUD::Spec, &block)
  test_paths.each do |path|

    spec = spec_cls.new(path)

    context(spec.description) do
      define_spec_tests_with_requirements(spec) do |req|
        define_crud_spec_test_examples(spec, req, &block)
      end
    end
  end
end
