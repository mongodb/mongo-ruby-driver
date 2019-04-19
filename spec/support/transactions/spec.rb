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

module Mongo
  module Transactions
    # Represents a Transactions specification test.
    #
    # @since 2.6.0
    class Spec < Mongo::CRUD::SpecBase

      # @return [ String ] description The spec description.
      #
      # @since 2.6.0
      attr_reader :description

      # Instantiate the new spec.
      #
      # @example Create the spec.
      #   Spec.new(file)
      #
      # @param [ String ] file The name of the file.
      #
      # @since 2.6.0
      def initialize(file)
        contents = ERB.new(File.read(file)).result

        # Since Ruby driver binds a client to a database, change the
        # database name in the spec to the one we are using
        contents.sub!(/"transaction-tests"/, '"ruby-driver"')
        contents.sub!(/"withTransaction-tests"/, '"ruby-driver"')
        # ... and collection name because we apparently hardcode that too
        contents.sub!(/"test"/, '"transactions-tests"')

        @spec = YAML.load(contents)
        @description = File.basename(file)
        @data = @spec['data']
        @transaction_tests = @spec['tests']

        super()
      end

      # Get a list of TransactionTests for each test definition.
      #
      # @example Get the list of TransactionTests.
      #   spec.tests
      #
      # @return [ Array<TransactionsTest> ] The list of TransactionTests.
      #
      # @since 2.6.0
      def tests
        @transaction_tests.map do |test|
          Proc.new { Mongo::Transactions::TransactionsTest.new(@data, test, self) }
        end.compact
      end

      def database_name
        @spec['database_name']
      end

      def collection_name
        @spec['collection_name']
      end

      def min_server_version
        @spec['minServerVersion']
      end
    end
  end
end
