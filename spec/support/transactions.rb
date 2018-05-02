# Copyright (C) 2014-2018 MongoDB, Inc.
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

# Matcher for determining if the results of the operation match the
# test's expected results.
#
# @since 2.6.0

# Matcher for determining if the collection's data matches the
# test's expected collection data.
#
# @since 2.6.0
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

require 'support/transactions/operation'

module Mongo
  module Transactions

    # Represents a Transactions specification test.
    #
    # @since 2.6.0
    class Spec

      # The name of the database to run the tests against.
      #
      # @since 2.6.0
      DATABASE_NAME = 'transaction-tests'.freeze

      # The name of the database to run the tests against.
      #
      # @since 2.6.0
      COLLECTION_NAME = 'test'.freeze

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
        file = File.new(file)
        @spec = YAML.load(ERB.new(file.read).result)
        file.close
        @description = File.basename(file)
        @data = @spec['data']
        @transaction_tests = @spec['tests']
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
        @transaction_tests.collect do |test|
          Mongo::Transactions::TransactionsTest.new(@data, test)
        end
      end
    end

    # Represents a single transaction test.
    #
    # @since 2.6.0
    class TransactionsTest

      # The test description.
      #
      # @return [ String ] description The test description.
      #
      # @since 2.6.0
      attr_reader :description

      # The expected command monitoring events
      #
      # @since 2.6.0
      attr_reader :expectations

      # Instantiate the new CRUDTest.
      #
      # @example Create the test.
      #   TransactionTest.new(data, test)
      #
      # @param [ Array<Hash> ] data The documents the collection
      # must have before the test runs.
      # @param [ Hash ] test The test specification.
      #
      # @since 2.6.0
      def initialize(data, test)
        @data = data
        @description = test['description']
        @client_options = convert_client_options(test['clientOptions'] || {})
        @session_options = symbolize_hash(test['sessionOptions'] || {})
        @failpoint = test['failPoint']
        @operations = test['operations']
        @expectations = test['expectations']
        @outcome = test['outcome']
        @expected_results = @operations.map do |o|
          result = o['result']
          next result unless result.class == Hash

          # Change maps of result ids to arrays of ids
          result.tap do |r|
            r.each do |k, v|
              next unless ['insertedIds', 'upsertedIds'].include?(k)
              r[k] = v.to_a.sort_by(&:first).map(&:last)
            end
          end
        end
      end

      # Run the test.
      #
      # @example Run the test.
      #   test.run
      #
      # @return [ Result ] The result of running the test.
      #
      # @since 2.6.0
      def run
        @collection.client.subscribe(Mongo::Monitoring::COMMAND, EventSubscriber.clear_events!)

        results = @ops.map { |o| o.execute(@collection, @session0, @session1) }

        session0_id = @session0.session_id
        session1_id = @session1.session_id

        @session0.end_session
        @session1.end_session

        events = EventSubscriber.started_events.map do |e|

          # Convert txnNumber field from a BSON integer to an extended JSON int64
          if e.command['txnNumber']
            e.command['txnNumber'] = {
              '$numberLong' => e.command['txnNumber'].instance_variable_get(:@integer).to_s
            }
          end

          # Replace the session id placeholders with the actual session ids.
          e.command['lsid'] = 'session0' if e.command['lsid'] == session0_id
          e.command['lsid'] = 'session1' if e.command['lsid'] == session1_id


          # The spec files don't include these fields, so we delete them.
          e.command.delete('$readPreference')
          e.command.delete('bypassDocumentValidation')
          e.command.delete('$clusterTime')


          if e.command['readConcern']
            # The spec test use an afterClusterTime value of 42 to indicate that we need to assert
            # that the field exists in the actual read concern rather than comparing the value, so
            # we replace any afterClusterTime value with 42.
            if e.command['readConcern']['afterClusterTime']
              e.command['readConcern']['afterClusterTime'] = 42
            end

            # Convert the readConcern level from a symbol to a string.
            if e.command['readConcern']['level']
              e.command['readConcern']['level'] = e.command['readConcern']['level'].to_s
            end
          end

          # The spec tests use 42 as a placeholder value for any getMore cursorId.
          e.command['getMore'] = { '$numberLong' => '42' } if e.command['getMore']

          # Remove fields if empty
          e.command.delete('cursor') if e.command['cursor'] && e.command['cursor'].empty?
          e.command.delete('filter') if e.command['filter'] && e.command['filter'].empty?
          e.command.delete('query') if e.command['query'] && e.command['query'].empty?

          {
            'command_started_event' => {
              'command' => e.command.to_a.sort,
              'command_name' => e.command_name.to_s,
              'database_name' => e.database_name
            }
          }
        end

        if @failpoint
          @collection.client.use('admin').command(configureFailPoint: 'failCommand', mode: 'off')
        end

        {
          results: results,
          contents: @collection.find.to_a,
          events: events
        }
      end

      def setup_test(address)
        client = Mongo::Client.new(address, database: :admin)

        begin
          client.command(killAllSessions: [])
        rescue Mongo::Error
        end

        db = client.use(Mongo::Transactions::Spec::DATABASE_NAME)
        coll = db[Mongo::Transactions::Spec::COLLECTION_NAME]
        coll.with(write: { w: :majority }).drop
        db.command(
          { create: Mongo::Transactions::Spec::COLLECTION_NAME },
          { write_concern: { w: :majority } })

        coll.with(write: { w: :majority }).insert_many(@data) unless @data.empty?
        client.command(@failpoint) if @failpoint

        client.close

        test_client = Mongo::Client.new(
          address,
          @client_options.merge(database: Mongo::Transactions::Spec::DATABASE_NAME))

        @collection = test_client[Mongo::Transactions::Spec::COLLECTION_NAME]

        @session0 = test_client.start_session(@session_options[:session0] || {})
        @session1 = test_client.start_session(@session_options[:session1] || {})

        @ops = @operations.reduce([]) do |ops, op|
          arguments = case op['arguments'] && op['arguments']['session']
                      when 'session0'
                        op['arguments'].merge('session' => @session0)
                      when 'session1'
                        op['arguments'].merge('session' => @session1)
                      else
                        op['arguments']
                      end

          ops << Operation.new(op.merge('arguments' => arguments))
        end
      end

      def teardown_test
        @collection.database.drop
        @collection.client.close
      end

      # Compare the existing collection data and the expected collection data.
      #
      # @example Compare the existing and expected collection data.
      #   test.compare_collection_data
      #
      # @return [ true, false ] The result of comparing the existing and expected
      #  collection data.
      #
      # @since 2.6.0
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
      # @since 2.6.0
      def compare_operation_result(actual_results)
        return false if @expected_results.length != actual_results.length

        @expected_results.zip(actual_results).all? do |expected, actual|
          compare_result(expected, actual)
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
      # @since 2.6.0
      def outcome_collection_data
        @outcome['collection']['data'] if @outcome && @outcome['collection']
      end

      private

      def convert_client_options(client_options)
        client_options.reduce({}) do |opts, kv|
          case kv.first
          when 'readConcernLevel'
            kv = [:read_concern, { 'level' => kv.last }]
          when 'readPreference'
            kv = [:read, { 'mode' => kv.last }]
          when 'w'
            kv = [:write, { w: kv.last }]
          else
            kv[0] = camel_to_snake(kv[0])
          end

          opts.tap { |o| o[kv.first] = kv.last }
        end
      end

      def compare_result(expected, actual)
        case expected
        when nil
          actual.nil?
        when Hash
          expected.all? do |k, v|
            case k
            when 'errorContains'
              actual && actual['errorContains'].include?(v)
            when 'errorLabelsContain'
              actual && v.all? { |label| actual['errorLabels'].include?(label) }
            when 'errorLabelsOmit'
              !actual || v.all? { |label| !actual['errorLabels'].include?(label) }
            else
              actual && (actual[k] == v || handle_upserted_id(k, v, actual[v]) ||
                handle_inserted_ids(k, v, actual[v]))
            end
          end
        else
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

      def handle_inserted_ids(field, expected, actual)
        if field == 'insertedIds'
          expected.values == actual
        end
      end

      def actual_collection_data
        if @outcome['collection']
          collection_name = @outcome['collection']['name'] || @collection.name
          @collection.database[collection_name].find.to_a
        end
      end
    end
  end
end
