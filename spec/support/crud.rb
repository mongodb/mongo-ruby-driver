# Copyright (C) 2014-2017 MongoDB, Inc.
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

# Matcher for determining if the results of the opeartion match the
# test's expected results.
#
# @since 2.0.0

# Matcher for determining if the collection's data matches the
# test's expected collection data.
#
# @since 2.0.0
RSpec::Matchers.define :match_collection_data do |test|

  match do
    test.compare_collection_data
  end
end

RSpec::Matchers.define :match_operation_result do |test|

  match do |actual|
    test.compare_operation_result(actual)
  end
end

require 'support/crud/read'
require 'support/crud/write'

module Mongo
  module CRUD

    # Represents a CRUD specification test.
    #
    # @since 2.0.0
    class Spec

      # @return [ String ] description The spec description.
      #
      # @since 2.0.0
      attr_reader :description

      # Instantiate the new spec.
      #
      # @example Create the spec.
      #   Spec.new(file)
      #
      # @param [ String ] file The name of the file.
      #
      # @since 2.0.0
      def initialize(file)
        file = File.new(file)
        @spec = YAML.load(ERB.new(file.read).result)
        file.close
        @description = File.basename(file)
        @data = @spec['data']
        @crud_tests = @spec['tests']
        @min_server_version = @spec['minServerVersion']
        @max_server_version = @spec['maxServerVersion']
      end


      # Whether the test can be run on a given server version.
      #
      # @example Can the test run on this server version?
      #   spec.server_version_satisfied?(client)
      #
      # @param [ Mongo::Client ] client The client to check.
      #
      # @return [ true, false ] Whether the test can be run on the given
      #   server version.
      #
      # @since 2.4.0
      def server_version_satisfied?(client)
        lower_bound_satisfied?(client) && upper_bound_satisfied?(client)
      end

      # Get a list of CRUDTests for each test definition.
      #
      # @example Get the list of CRUDTests.
      #   spec.tests
      #
      # @return [ Array<CRUDTest> ] The list of CRUDTests.
      #
      # @since 2.0.0
      def tests
        @crud_tests.collect do |test|
          Mongo::CRUD::CRUDTest.new(@data, test)
        end
      end

      private

      def upper_bound_satisfied?(client)
        if @max_server_version
          if @max_server_version < '2.6'
            !client.cluster.next_primary.features.write_command_enabled?
          end
        else
          true
        end
      end

      def lower_bound_satisfied?(client)
        if @min_server_version
          if @min_server_version >= '3.4'
            client.cluster.next_primary.features.collation_enabled?
          elsif @min_server_version >= '2.6'
            client.cluster.next_primary.features.write_command_enabled?
          else
            true
          end
        else
          true
        end
      end
    end

    # Represents a single CRUD test.
    #
    # @since 2.0.0
    class CRUDTest

      # The test description.
      #
      # @return [ String ] description The test description.
      #
      # @since 2.0.0
      attr_reader :description

      # Instantiate the new CRUDTest.
      #
      # @example Create the test.
      #   CRUDTest.new(data, test)
      #
      # @param [ Array<Hash> ] data The documents the collection
      # must have before the test runs.
      # @param [ Hash ] test The test specification.
      #
      # @since 2.0.0
      def initialize(data, test)
        @data = data
        @description = test['description']
        @operation = Operation.get(test['operation'])
        @outcome = test['outcome']
      end

      # Run the test.
      #
      # @example Run the test.
      #   test.run(collection)
      #
      # @param [ Collection ] collection The collection the test
      #   should be run on.
      #
      # @return [ Result, Array<Hash> ] The result(s) of running the test.
      #
      # @since 2.0.0
      def run(collection)
        @collection = collection
        @collection.insert_many(@data)
        @operation.execute(collection)
      end

      # The expected result of running the test.
      #
      # @example Get the expected result of running the test.
      #   test.result
      #
      # @return [ Array<Hash> ] The expected result of running the test.
      #
      # @since 2.0.0
      def result
        @operation.has_results? ? @outcome['result'] : []
      end

      # Compare the existing collection data and the expected collection data.
      #
      # @example Compare the existing and expected collection data.
      #   test.compare_collection_data
      #
      # @return [ true, false ] The result of comparing the existing and expected
      #  collection data.
      #
      # @since 2.0.0
      def compare_collection_data
        if actual_collection_data.nil?
          outcome_collection_data.nil?
        elsif actual_collection_data.empty?
          outcome_collection_data.empty?
        else
          actual_collection_data.all? do |doc|
            outcome_collection_data.include?(doc)
          end
        end
      end

      # Compare the actual operation result to the expected operation result.
      #
      # @example Compare the existing and expected operation results.
      #   test.compare_operation_result(actual_results)
      #
      # @params [ Object ] actual The actual test results.
      #
      # @return [ true, false ] The result of comparing the expected and actual operation result.
      #
      # @since 2.4.0
      def compare_operation_result(actual)
        if actual.is_a?(Array)
          actual.empty? || @outcome['result'].each_with_index do |expected, i|
            compare_result(expected, actual[i])
          end
        else
          compare_result(@outcome['result'], actual)
        end
      end

      # The expected data in the collection as an outcome after running this test.
      #
      # @example Get the outcome collection data
      #   test.outcome_collection_data
      #
      # @return [ Array<Hash> ] The list of documents expected to be in the collection
      #   after running this test.
      #
      # @since 2.4.0
      def outcome_collection_data
        @outcome['collection']['data'] if @outcome['collection']
      end

      private

      def compare_result(expected, actual)
        case expected
          when nil
            actual.nil?
          when Hash
            actual.all? do |k, v|
              expected[k] == v || handle_upserted_id(k, expected[k], v)
            end
          when Integer
            expected == actual
        end
      end

      def handle_upserted_id(field, expected_id, actual_id)
        return true if expected_id.nil?
        if field == 'upsertedId'
          if expected_id.is_a?(Integer)
            actual_id.is_a?(BSON::ObjectId) || actual_id.nil?
          end
        end
      end

      def actual_collection_data
        if @outcome['collection']
          collection_name = @outcome['collection']['name'] || @collection.name
          @collection.database[collection_name].find.to_a
        end
      end
    end

    # Helper module for instantiating either a Read or Write test operation.
    #
    # @since 2.0.0
    module Operation
      extend self

      # Get a new Operation.
      #
      # @example Get the operation.
      #   Operation.get(spec)
      #
      # @param [ Hash ] spec The operation specification.
      #
      # @return [ Operation::Write, Operation::Read ] The Operation object.
      #
      # @since 2.0.0
      def get(spec)
        if Write::OPERATIONS.keys.include?(spec['name'])
          Write.new(spec)
        else
          Read.new(spec)
        end
      end
    end
  end
end
