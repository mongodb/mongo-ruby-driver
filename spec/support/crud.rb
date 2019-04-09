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

def define_crud_spec_test_examples(spec, req = nil, &block)
  spec.tests.each do |test|

    context(test.description) do

      before(:each) do
        unless spec.server_version_satisfied?(client)
          skip 'Version requirement not satisfied'
        end
      end

      let(:verifier) { Mongo::CRUD::Verifier.new(test) }

      instance_exec(spec, req, test, &block)

      test.operations.each_with_index do |operation, index|
        context "operation #{index+1}" do

          let(:result) do
            if operation.outcome.error?
              error = nil
              begin
                test.run(collection, index+1)
              rescue => e
                error = e
              end
              error
            else
              test.run(collection, index+1)
            end
          end

          let(:verify_collection_name) do
            if operation.outcome && operation.outcome.collection_name
              operation.outcome.collection_name
            else
              'crud_spec_test'
            end
          end

          let(:verify_collection) { client[verify_collection_name] }

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
    end
  end
end

def define_crud_spec_tests(description, test_paths, &block)
  describe(description) do

    test_paths.each do |path|

      spec = Mongo::CRUD::Spec.new(path)

      context(spec.description) do
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

              define_crud_spec_test_examples(spec, req, &block)
            end
          end
        else
          define_crud_spec_test_examples(spec, &block)
        end
      end
    end
  end
end
