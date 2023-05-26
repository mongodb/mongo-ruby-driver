# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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
    class TransactionsTest < CRUD::CRUDTestBase
      include MongosMacros

      attr_reader :expected_results
      attr_reader :skip_reason

      attr_reader :results

      # @return [ Crud::Spec ] the top-level YAML specification object
      attr_reader :spec

      # Instantiate the new CRUDTest.
      #
      # @example Create the test.
      #   TransactionTest.new(data, test)
      #
      # @param [ Crud::Spec ] crud_spec The top level YAML specification object.
      # @param [ Array<Hash> ] data The documents the collection
      # must have before the test runs.
      # @param [ Hash ] test The test specification.
      # @param [ true | false | Proc ] expectations_bson_types Whether bson
      #   types should be expected. If a Proc is given, it is invoked with the
      #   test as its argument, and should return true or false.
      #
      # @since 2.6.0
      def initialize(crud_spec, data, test, expectations_bson_types: true)
        test = IceNine.deep_freeze(test)
        @spec = crud_spec
        @data = data || []
        @description = test['description']
        @client_options = {
          # Disable legacy read & write retries, so that when spec tests
          # disable modern retries we do not retry at all instead of using
          # legacy retries which is contrary to what the tests want.
          max_read_retries: 0,
          max_write_retries: 0,
          app_name: 'Tx spec - test client',
        }.update(::Utils.convert_client_options(test['clientOptions'] || {}))

        @fail_point_command = test['failPoint']

        @session_options = if opts = test['sessionOptions']
          Hash[opts.map do |session_name, options|
            [session_name.to_sym, ::Utils.convert_operation_options(options)]
          end]
        else
          {}
        end
        @skip_reason = test['skipReason']
        @multiple_mongoses = test['useMultipleMongoses']

        operations = test['operations']
        @operations = operations.map do |op|
          Operation.new(self, op)
        end

        if expectations_bson_types.respond_to?(:call)
          expectations_bson_types = expectations_bson_types[self]
        end

        mode = if expectations_bson_types then :bson else nil end
        @expectations = BSON::ExtJSON.parse_obj(test['expectations'], mode: mode)

        if test['outcome']
          @outcome = Mongo::CRUD::Outcome.new(BSON::ExtJSON.parse_obj(test['outcome'], mode: mode))
        end

        @expected_results = operations.map do |o|
          o = BSON::ExtJSON.parse_obj(o, mode: :bson)

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
        @test_client ||= begin
          sdam_proc = lambda do |test_client|
            test_client.subscribe(Mongo::Monitoring::COMMAND, command_subscriber)

            test_client.subscribe(Mongo::Monitoring::TOPOLOGY_OPENING, sdam_subscriber)
            test_client.subscribe(Mongo::Monitoring::SERVER_OPENING, sdam_subscriber)
            test_client.subscribe(Mongo::Monitoring::SERVER_DESCRIPTION_CHANGED, sdam_subscriber)
            test_client.subscribe(Mongo::Monitoring::TOPOLOGY_CHANGED, sdam_subscriber)
            test_client.subscribe(Mongo::Monitoring::SERVER_CLOSED, sdam_subscriber)
            test_client.subscribe(Mongo::Monitoring::TOPOLOGY_CLOSED, sdam_subscriber)
            test_client.subscribe(Mongo::Monitoring::CONNECTION_POOL, sdam_subscriber)
          end

          if kms_providers = @client_options.dig(:auto_encryption_options, :kms_providers)
            @client_options[:auto_encryption_options][:kms_providers] = kms_providers.map do |provider, opts|
              case provider
              when :aws_temporary
                [
                  :aws,
                  {
                    access_key_id: SpecConfig.instance.fle_aws_temp_key,
                    secret_access_key: SpecConfig.instance.fle_aws_temp_secret,
                    session_token: SpecConfig.instance.fle_aws_temp_session_token,
                  }
                ]
              when :aws_temporary_no_session_token
                [
                  :aws,
                  {
                    access_key_id: SpecConfig.instance.fle_aws_temp_key,
                    secret_access_key: SpecConfig.instance.fle_aws_temp_secret,
                  }
                ]
              else
                [provider, opts]
              end
            end.to_h
          end

          if @client_options[:auto_encryption_options] && SpecConfig.instance.crypt_shared_lib_path
            @client_options[:auto_encryption_options][:extra_options] ||= {}
            @client_options[:auto_encryption_options][:extra_options][:crypt_shared_lib_path] = SpecConfig.instance.crypt_shared_lib_path
          end

          ClientRegistry.instance.new_local_client(
            SpecConfig.instance.addresses,
            SpecConfig.instance.authorized_test_options.merge(
              database: @spec.database_name,
              auth_source: SpecConfig.instance.auth_options[:auth_source] || 'admin',
              sdam_proc: sdam_proc,
            ).merge(@client_options))
        end
      end

      def command_subscriber
        @command_subscriber ||= Mrss::EventSubscriber.new
      end

      def sdam_subscriber
        @sdam_subscriber ||= Mrss::EventSubscriber.new(name: 'sdam subscriber')
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
        @threads = {}

        results = @operations.map do |op|
          target = resolve_target(test_client, op)
          if op.needs_session?
            context = CRUD::Context.new(
              session0: session0,
              session1: session1,
              sdam_subscriber: sdam_subscriber,
              threads: @threads,
              primary_address: @primary_address,
            )
          else
            # Hack to support write concern operations tests, which are
            # defined to use transactions format but target pre-3.6 servers
            # that do not support sessions
            target ||= support_client
            context = CRUD::Context.new(
              sdam_subscriber: sdam_subscriber,
              threads: @threads,
              primary_address: @primary_address,
            )
          end

          op.execute(target, context).tap do
            @threads = context.threads
            @primary_address = context.primary_address
          end
        end

        session0_id = @session0&.session_id
        session1_id = @session1&.session_id

        @session0&.end_session
        @session1&.end_session

        actual_events = ::Utils.yamlify_command_events(command_subscriber.started_events)
        actual_events = actual_events.reject do |event|
          event['command_started_event']['command']['endSessions']
        end
        actual_events.each do |e|

          # Replace the session id placeholders with the actual session ids.
          payload = e['command_started_event']
          if @session0
            payload['command']['lsid'] = 'session0' if payload['command']['lsid'] == session0_id
          end
          if @session1
            payload['command']['lsid'] = 'session1' if payload['command']['lsid'] == session1_id
          end

        end

        @results = {
          results: results,
          contents: @result_collection.with(
            read: {mode: 'primary'},
            read_concern: { level: 'local' },
          ).find.sort(_id: 1).to_a,
          events: actual_events,
        }
      end

      def setup_test
        begin
          admin_support_client.command(killAllSessions: [])
        rescue Mongo::Error
        end

        if ClusterConfig.instance.fcv_ish >= '4.2'
          ::Utils.mongos_each_direct_client do |direct_client|
            direct_client.command(configureFailPoint: 'failCommand', mode: 'off')
          end
        end

        key_vault_coll = support_client
        .use(:keyvault)[:datakeys]
        .with(write: { w: :majority })

        key_vault_coll.drop
        # Insert data into the key vault collection if required to do so by
        # the tests.
        if @spec.key_vault_data && !@spec.key_vault_data.empty?
          key_vault_coll.insert_many(@spec.key_vault_data)
        end

        encrypted_fields = @spec.encrypted_fields if @spec.encrypted_fields
        coll = support_client[@spec.collection_name].with(write: { w: :majority })
        coll.drop(encrypted_fields: encrypted_fields)

        # Place a jsonSchema validator on the collection if required to do so
        # by the tests.
        collection_validator = if @spec.json_schema
          { '$jsonSchema' => @spec.json_schema }
        else
          {}
        end

        create_collection_spec = {
          create: @spec.collection_name,
          validator: collection_validator,
          writeConcern: { w: 'majority' }
        }

        create_collection_spec[:encryptedFields] = encrypted_fields if encrypted_fields
        support_client.command(create_collection_spec)

        coll.insert_many(@data) unless @data.empty?

        if description =~ /distinct/ || @operations.any? { |op| op.name == 'distinct' }
          run_mongos_distincts(@spec.database_name, 'test')
        end

        admin_support_client.command(@fail_point_command) if @fail_point_command

        @collection = test_client[@spec.collection_name]

        # Client-side encryption tests require the use of a separate client
        # without auto_encryption_options for querying results.
        result_collection_name = outcome&.collection_name || @spec.collection_name
        @result_collection = support_client.use(@spec.database_name)[result_collection_name]
      end

      def teardown_test

        if @fail_point_command
          admin_support_client.command(configureFailPoint: 'failCommand', mode: 'off')
        end

        if $disable_fail_points
          $disable_fail_points.each do |(fail_point_command, address)|
            client = ClusterTools.instance.direct_client(address,
              database: 'admin')
            client.command(configureFailPoint: fail_point_command['configureFailPoint'],
              mode: 'off')
          end
          $disable_fail_points = nil
        end

        if @test_client
          @test_client.cluster.session_pool.end_sessions
        end
      end

      def resolve_target(client, operation)
        case operation.object
        when 'session0'
          session0
        when 'session1'
          session1
        when 'testRunner'
          # We don't actually use this target in any way.
          nil
        else
          super
        end
      end

      def session0
        @session0 ||= test_client.start_session(@session_options[:session0] || {})
      end

      def session1
        @session1 ||= test_client.start_session(@session_options[:session1] || {})
      end
    end
  end
end
