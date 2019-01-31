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

require 'support/crud/read'
require 'support/crud/write'
require 'support/crud/verifier'

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
        return true unless @max_server_version
        client.database.command(buildInfo: 1).first['version'] <= @max_server_version
      end

      def lower_bound_satisfied?(client)
        return true unless @min_server_version
        #@min_server_version <= client.database.command(buildInfo: 1).first['version']
        @min_server_version <= ClusterConfig.instance.fcv_ish
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

      # Spec tests have configureFailPoint as a string, make it a string here too
      FAIL_POINT_BASE_COMMAND = {
        'configureFailPoint' => "onPrimaryTransactionalWrite",
      }.freeze

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
        if test['failPoint']
          @fail_point_command = FAIL_POINT_BASE_COMMAND.merge(test['failPoint'])
        end
        @description = test['description']
        @operation = Operation.get(test['operation'])
        @outcome = test['outcome']
      end

      attr_reader :outcome

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
        @operation.execute(collection)
      end

      def setup_test(collection)
        clear_fail_point(collection)
        @collection = collection
        collection.delete_many
        collection.insert_many(@data)
        set_up_fail_point(collection)
      end

      def set_up_fail_point(collection)
        if @fail_point_command
          collection.client.use(:admin).command(@fail_point_command)
        end
      end

      def clear_fail_point(collection)
        if @fail_point_command
          collection.client.use(:admin).command(FAIL_POINT_BASE_COMMAND.merge(mode: "off"))
        end
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

      def error?
        !!@outcome['error']
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
