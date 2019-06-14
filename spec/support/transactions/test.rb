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

      attr_reader :expected_results
      attr_reader :skip_reason

      attr_reader :results

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
        @client_options = Utils.convert_client_options(test['clientOptions'] || {})
        @session_options = Utils.snakeize_hash(test['sessionOptions'] || {})
        @fail_point = test['failPoint']
        @operations = test['operations']
        @expectations = test['expectations']
        @skip_reason = test['skipReason']
        @multiple_mongoses = test['useMultipleMongoses']
        if test['outcome']
          @outcome = Mongo::CRUD::Outcome.new(test['outcome'])
        end
        @expected_results = @operations.map do |o|
          # We check both o.key('error') and o['error'] to provide a better
          # error message in case error: false is ever needed in the tests
          if o.key?('error')
            if o['error']
              {'error' => true}
            else
              raise "Unsupported error value #{o['error']}"
            end
          else
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
      end

      attr_reader :outcome

      def multiple_mongoses?
        @multiple_mongoses
      end

      def support_client
        @support_client ||= ClientRegistry.instance.global_client('root_authorized').use(@spec.database_name)
      end

      def admin_support_client
        @admin_support_client ||= support_client.use('admin')
      end

      def test_client
        @test_client ||= ClientRegistry.instance.global_client(
          'authorized_without_retry_writes'
        ).with(@client_options.merge(
          database: @spec.database_name,
        ))
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

        $distinct_ran ||= if @ops.any? { |op| op.name == 'distinct' }
          if ClusterConfig.instance.mongos?
            client = ClientRegistry.instance.global_client('basic')
            client.cluster.next_primary
            client.cluster.servers.each do |server|
              direct_client = ClientRegistry.instance.new_local_client(
                [server.address.to_s],
                SpecConfig.instance.test_options.merge(
                  connect: :sharded
                ).merge(SpecConfig.instance.auth_options))
              direct_client['test'].distinct('foo').to_a
            end
          end
        end

        results = @ops.map do |op|
          op.execute(@collection, @session0, @session1)
        end

        session0_id = @session0.session_id
        session1_id = @session1.session_id

        @session0.end_session
        @session1.end_session

        actual_events = Utils.yamlify_command_events(event_subscriber.started_events)
        actual_events = actual_events.reject do |event|
          event['command_started_event']['command']['endSessions']
        end
        actual_events.each do |e|

          # Replace the session id placeholders with the actual session ids.
          payload = e['command_started_event']
          payload['command']['lsid'] = 'session0' if payload['command']['lsid'] == session0_id
          payload['command']['lsid'] = 'session1' if payload['command']['lsid'] == session1_id

        end

        @results = {
          results: results,
          contents: @collection.with(
          read: {mode: 'primary'},
            read_concern: { level: 'local' },
          ).find.to_a,
          events: actual_events,
        }
      end

      def setup_test
        begin
          admin_support_client.command(killAllSessions: [])
        rescue Mongo::Error
        end

        coll = support_client[@spec.collection_name].with(write: { w: :majority })
        coll.drop
        support_client.command(create: @spec.collection_name, writeConcern: { w: :majority })

        coll.insert_many(@data) unless @data.empty?
        admin_support_client.command(@fail_point) if @fail_point

        @collection = test_client[@spec.collection_name]

        @session0 = test_client.start_session(@session_options[:session0] || {})
        @session1 = test_client.start_session(@session_options[:session1] || {})

        @ops = @operations.map do |op|
          Operation.new(op)
        end
      end

      def teardown_test

        if @fail_point
          admin_support_client.command(configureFailPoint: 'failCommand', mode: 'off')
        end

        if @test_client
          @test_client.cluster.session_pool.end_sessions
        end
      end
    end
  end
end
