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

require_relative './gridfs'

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
        @topologies = if topologies = @spec['topology']
          topologies.map do |topology|
            {'replicaset' => :replica_set, 'single' => :single, 'sharded' => :sharded}[topology]
          end
        else
          nil
        end
      end

      attr_reader :min_server_version
      attr_reader :topologies

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
      # data can be an array of hashes, with each hash corresponding to a
      # document to be inserted into the collection whose name is given in
      # collection_name as configured in the YAML file. Alternatively data
      # can be a map of collection names to arrays of hashes.
      #
      # @param [ Hash | Array<Hash> ] data The documents the collection
      #   must have before the test runs.
      # @param [ Hash ] test The test specification.
      #
      # @since 2.0.0
      def initialize(data, test)
        @data = data
        if test['failPoint']
          @fail_point_command = FAIL_POINT_BASE_COMMAND.merge(test['failPoint'])
        end
        @description = test['description']
        @client_options = Utils.convert_client_options(test['clientOptions'] || {})
        if test['operations']
          @operations = test['operations'].map do |op_spec|
            Operation.get(op_spec)
          end
        else
          @operations = [Operation.get(test['operation'], test['outcome'])]
        end
      end

      attr_reader :client_options

      # Operations to be performed by the test.
      #
      # For CRUD tests, there is one operation for test. For retryable writes,
      # there are multiple operations for each test. In either case we build
      # an array of operations.
      attr_reader :operations

      # Run the test.
      #
      # The specified number of operations are executed, so that the
      # test can assert on the outcome of each specified operation in turn.
      #
      # @param [ Collection ] collection The collection the test
      #   should be run on.
      # @param [ Integer ] num_ops Number of operations to run.
      #
      # @return [ Result, Array<Hash> ] The result(s) of running the test.
      #
      # @since 2.0.0
      def run(collection, num_ops)
        result = nil
        1.upto(num_ops) do |i|
          operation = @operations[i-1]
          target = case operation.object
          when 'collection'
            collection
          when 'database'
            collection.database
          when 'client'
            collection.client
          when 'gridfsbucket'
            collection.database.fs
          else
            raise "Unknown target #{operation.object}"
          end
          result = operation.execute(target)
        end
        result
      end

      class DataConverter
        include Mongo::GridFS::Convertible
      end

      def setup_test(collection)
        client = collection.client
        clear_fail_point(client)
        if @data.is_a?(Array)
          @collection = collection
          collection.delete_many
          collection.insert_many(@data)
        elsif @data.is_a?(Hash)
          converter = DataConverter.new
          @data.each do |collection_name, data|
            collection = client[collection_name]
            collection.delete_many
            data = converter.transform_docs(data)
            collection.insert_many(data)
          end
        else
          raise "Unknown type of data: #{@data}"
        end
        set_up_fail_point(client)
      end

      def set_up_fail_point(client)
        if @fail_point_command
          client.use(:admin).command(@fail_point_command)
        end
      end

      def clear_fail_point(client)
        if @fail_point_command
          client.use(:admin).command(FAIL_POINT_BASE_COMMAND.merge(mode: "off"))
        end
      end

      private

      def actual_collection_data
        if expected_outcome.collection_data?
          collection_name = expected_outcome.collection_name || @collection.name
          @collection.database[collection_name].find.to_a
        end
      end
    end
  end
end

require 'support/crud/outcome'
require 'support/crud/operation'
require 'support/crud/read'
require 'support/crud/write'
require 'support/crud/verifier'
