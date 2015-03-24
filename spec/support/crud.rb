# Copyright (C) 2014-2015 MongoDB, Inc.
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
RSpec::Matchers.define :match_results do |test|

  match do |actual|
    actual == test.result
  end
end

# Matcher for determining if the collection's data matches the
# test's expected collection data.
#
# @since 2.0.0
RSpec::Matchers.define :match_collection_data do |test|

  match do |actual|
    test.compare_collection_data
  end
end

require 'support/crud/readable'

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
        @spec = YAML.load(ERB.new(File.new(file).read).result)
        @description = file
        @data = @spec['data']
        @crud_tests = @spec['tests']
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
    end

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
      # should have before the test runs.
      # @param [ Hash ] test The test specification.
      #
      # @since 2.0.0
      def initialize(data, test)
        @data = data
        @description = test['description']
        @operation = Operation.new(test['operation'])
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
        @collection.find.delete_many
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
        actual_collection_data == outcome_collection_data
      end

      private

      def outcome_collection_data
        @outcome['collection']['data'] if @outcome['collection']
      end

      def actual_collection_data
        @collection.database[@outcome['collection']['name']].find.to_a if @outcome['collection']
      end
    end

    class Operation
      include Readable

      # The operation name.
      #
      # @return [ String ] name The operation name.
      #
      # @since 2.0.0
      attr_reader :name

      # Instantiate the new Operation.
      #
      # @example Create the operation.
      #   Operation.new(spec)
      #
      # @param [ Hash ] spec The operation specification.
      #
      # @since 2.0.0
      def initialize(spec)
        @spec = spec
        @name = @spec['name']
      end

      # Execute the operation.
      #
      # @example Execute the operation.
      #   operation.execute
      #
      # @param [ Collection ] collection The collection the operation
      #   should be executed on.
      #
      # @return [ Result, Array<Hash> ] The result of executing the operation.
      #
      # @since 2.0.0
      def execute(collection)
        send(name.to_sym, collection)
      end
    end
  end
end
