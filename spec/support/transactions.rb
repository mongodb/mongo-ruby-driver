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

# Matcher for determining if the results of the operation match the
# test's expected results.
#
# @since 2.6.0
RSpec::Matchers.define :match_operation_result do |test|

  match do |actual|
    rv = test.compare_operation_result(actual)
    puts rv
    rv.nil?
  end
end

require 'support/transactions/operation'

module Mongo
  module Transactions

    # Represents a Transactions specification test.
    #
    # @since 2.6.0
    class Spec

      # The name of the collection to run the tests against.
      #
      # @since 2.6.0
      COLLECTION_NAME = 'transactions-tests'.freeze

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
        contents = ERB.new(file.read).result
        @spec = YAML.load(contents)
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
        @transaction_tests.map do |test|
          if test['skipReason']
            nil
          else
            Proc.new { Mongo::Transactions::TransactionsTest.new(@data, test, self) }
          end
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
      # @param [ Hash ] spec The top level YAML specification.
      #
      # @since 2.6.0
      def initialize(data, test, spec)
        test = IceNine.deep_freeze(test)
        @spec = spec
        @data = data
        @description = test['description']
        @client_options = convert_client_options(test['clientOptions'] || {})
        @session_options = snakeize_hash(test['sessionOptions'] || {})
        @fail_point = test['failPoint']
        @operations = test['operations']
        @expectations = test['expectations']
        @outcome = test['outcome']
        @expected_results = @operations.map do |o|
          result = o['result']
          next result unless result.class == Hash

          # Change maps of result ids to arrays of ids
          result.dup.tap do |r|
            r.each do |k, v|
              next unless ['insertedIds', 'upsertedIds'].include?(k)
              r[k] = v.to_a.sort_by(&:first).map(&:last)
            end
          end
        end
      end

      def support_client
        @support_client ||= ClientRegistry.instance.global_client('root_authorized').use(@spec.database_name)
      end

      def admin_support_client
        @admin_support_client ||= support_client.use('admin')
      end

      def test_client
        @test_client ||= ClientRegistry.instance.global_client('authorized_without_retry_writes').with(
          @client_options.merge(
            database: @spec.database_name,
            app_name: 'this is used solely to force the new client to create its own cluster'))
      end

      def event_subscriber
        @event_subscriber ||= EventSubscriber.new
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
        test_client.subscribe(Mongo::Monitoring::COMMAND, event_subscriber)

        results = @ops.map { |o| o.execute(@collection) }

        session0_id = @session0.session_id
        session1_id = @session1.session_id

        @session0.end_session
        @session1.end_session

        events = event_subscriber.started_events.map do |e|

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

          # This write concern is sent for some server topologies/configurations, but not all, so it
          # doesn't appear in the expected events.
          e.command.delete('writeConcern') if e.command['writeConcern'] == { 'w' => 2 }

          # The spec tests use 42 as a placeholder value for any getMore cursorId.
          e.command['getMore'] = { '$numberLong' => '42' } if e.command['getMore']

          # Remove fields if empty
          e.command.delete('cursor') if e.command['cursor'] && e.command['cursor'].empty?
          e.command.delete('filter') if e.command['filter'] && e.command['filter'].empty?
          e.command.delete('query') if e.command['query'] && e.command['query'].empty?

          {
            'command_started_event' => {
              'command' => order_hash(e.command),
              'command_name' => e.command_name.to_s,
              'database_name' => e.database_name
            }
          }
        end

        # Remove any events from authentication commands.
        events.reject! { |c| c['command_started_event']['command_name'].start_with?('sasl') }

        if @fail_point
          admin_support_client.command(configureFailPoint: 'failCommand', mode: 'off')
        end

        events.map! do |event|
          event['command_started_event'] = order_hash(event['command_started_event'])
        end

        {
          results: results,
          contents: @collection.find.to_a,
          events: events,
        }
      end

      def setup_test
        begin
          admin_support_client.command(killAllSessions: [])
        rescue Mongo::Error
        end

        coll = support_client[@spec.collection_name]
        coll.database.drop
        coll.with(write: { w: :majority }).drop
        support_client.command(
          { create: @spec.collection_name },
          { write_concern: { w: :majority } })

        coll.with(write: { w: :majority }).insert_many(@data) unless @data.empty?
        admin_support_client.command(@fail_point) if @fail_point

        @collection = test_client[@spec.collection_name]

        @session0 = test_client.start_session(@session_options[:session0] || {})
        @session1 = test_client.start_session(@session_options[:session1] || {})

        @ops = @operations.map do |op|
          Operation.new(op, @session0, @session1)
        end
      end

      def teardown_test
        if @admin_support_client
          @admin_support_client.close
        end
        if @test_client
          @test_client.close
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
        if @expected_results.length != actual_results.length
          return "Numbers of results differ: expected #{@expected_results.length}, got #{actual_results.length}"
        end

        index = 0
        @expected_results.zip(actual_results).all? do |expected, actual|
          if expected
            rv = compare_result(expected, actual)
            if rv
              return "Results differ at index #{index}: #{rv}"
            end
          end
          index += 1
        end

        nil
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

      def order_hash(hash)
        Hash[hash.to_a.sort]
      end

      def verifier
        Verifier.new(self)
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
          if actual.nil?
            nil
          else
            "Expected nil, got #{actual}"
          end
        when Hash
          expected.all? do |k, v|
            case k
            when 'errorContains'
              if actual && actual['errorContains'].include?(v)
                nil
              else
                "Expected #{actual['errorContains']} to include #{v}"
              end
            when 'errorLabelsContain'
              if actual && v.all? { |label| actual['errorLabels'].include?(label) }
                nil
              else
                "Expected #{actual} to include error labels #{v.inspect}"
              end
            when 'errorLabelsOmit'
              if !actual || v.all? { |label| !actual['errorLabels'].include?(label) }
                nil
              else
                "Expected #{actual} to not include error label #{v.inspect}"
              end
            else
              if actual && (actual[k] == v || handle_upserted_id(k, v, actual[v]) ||
                handle_inserted_ids(k, v, actual[v]))
              then
                nil
              else
                "Expected #{actual} to match inserted ids in #{v}}"
              end
            end
          end
        else
          if expected == actual
            nil
          else
            "Expected #{actual} to equal #{expected}"
          end
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

      class Verifier
        include RSpec::Matchers

        def initialize(test_instance)
          @test_instance = test_instance
        end

        attr_reader :test_instance

        def verify_command_started_event_count(results)
          expectations = test_instance.expectations
          expect(results[:events].length).to eq(expectations.length)
        end

        def verify_command_started_event(results, i)
          expectation = test_instance.expectations[i]

          expect(expectation.keys).to eq(%w(command_started_event))
          expected_event = expectation['command_started_event'].dup
          actual_event = results[:events][i].dup
          expect(actual_event).not_to be nil
          expect(expected_event.keys).to eq(actual_event.keys)

          expected_command = expected_event.delete('command')
          actual_command = actual_event.delete('command')

          # Hash#compact is ruby 2.4+
          expected_presence = expected_command.select { |k, v| !v.nil? }
          expected_absence = expected_command.select { |k, v| v.nil? }

          expect(actual_command).to eq(expected_presence)
          expected_absence.each do |k, v|
            expect(actual_command).not_to have_key(k)
          end

          # this compares remaining fields in events after command is removed
          expect(expected_event).to eq(actual_event)
        end
      end
    end
  end
end
