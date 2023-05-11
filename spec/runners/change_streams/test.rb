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

require 'runners/crud/operation'
require 'runners/crud/test_base'
require 'runners/change_streams/outcome'

module Mongo
  module ChangeStreams

    class ChangeStreamsTest < Mongo::CRUD::CRUDTestBase

      def initialize(crud_spec, test, collection_name, collection2_name, database_name, database2_name)
        @spec = crud_spec
        @description = test['description']

        @fail_point_command = test['failPoint']

        @min_server_version = test['minServerVersion']
        @max_server_version = test['maxServerVersion']
        @target_type = test['target']
        @topologies = test['topology'].map do |topology|
          {'single' => :single, 'replicaset' => :replica_set, 'sharded' => :sharded}[topology]
        end
        @pipeline = test['changeStreamPipeline'] || []
        @options = test['changeStreamOptions'] || {}

        @operations = test['operations'].map do |op|
          Mongo::CRUD::Operation.new(self, op)
        end

        @expectations = test['expectations'] &&
          BSON::ExtJSON.parse_obj(test['expectations'], mode: :bson)

        @result = BSON::ExtJSON.parse_obj(test['result'], mode: :bson)
        @collection_name = collection_name
        @collection2_name = collection2_name
        @database_name = database_name
        @database2_name = database2_name

        @outcome = Outcome.new(test.fetch('result'))
      end

      attr_reader :topologies

      attr_reader :outcome

      attr_reader :result

      def setup_test
        clear_fail_point(global_client)

        @database = global_client.use(@database_name).database.tap(&:drop)
        if @database2_name
          @database2 = global_client.use(@database2_name).database.tap(&:drop)
        end

        # Work around https://jira.mongodb.org/browse/SERVER-17397
        if ClusterConfig.instance.server_version < '4.4' &&
          global_client.cluster.servers.length > 1
        then
          ::Utils.mongos_each_direct_client do |client|
            client.database.command(flushRouterConfig: 1)
          end
        end

        @database[@collection_name].create
        if @collection2_name
          @database2[@collection2_name].create
        end

        client = ClientRegistry.instance.global_client('root_authorized').with(
          database: @database_name,
          app_name: 'this is used solely to force the new client to create its own cluster')

        setup_fail_point(client)

        @subscriber = Mrss::EventSubscriber.new
        client.subscribe(Mongo::Monitoring::COMMAND, @subscriber)

        @target = case @target_type
                 when 'client'
                   client
                 when 'database'
                   client.database
                 when 'collection'
                   client[@collection_name]
                 end
      end

      def teardown_test
        if @fail_point_command
          clear_fail_point(global_client)
        end
      end

      def run
        change_stream = begin
          @target.watch(@pipeline, ::Utils.snakeize_hash(@options))
        rescue Mongo::Error::OperationFailure => e
          return {
            result: {
              error: {
                code: e.code,
                labels: e.labels,
              },
            },
            events: events,
          }
        end

        # JRuby must iterate the same object, not switch from
        # enum to change stream
        enum = change_stream.to_enum

        @operations.each do |op|
          db = case op.spec['database']
            when @database_name
              @database
            when @database2_name
              @database2
            else
              raise "Unknown database name #{op.spec['database']}"
            end
          collection = db[op.spec['collection']]
          op.execute(collection)
        end

        changes = []

        # attempt first next call (catch NonResumableChangeStreamError errors)
        begin
          change = enum.next
          changes << change
        rescue Mongo::Error::OperationFailure => e
          return {
            result: {
              error: {
                code: e.code,
                labels: e.labels,
              },
            },
            events: events,
          }
        end

        # continue until changeStream has received as many changes as there
        # are in result.success
        if @result['success'] && changes.length < @result['success'].length
          while changes.length < @result['success'].length
            changes << enum.next
          end
        end

        change_stream.close

        {
          result: { 'success' => changes },
          events: events,
        }
      end

      def server_version_satisfied?(client)
        lower_bound_satisfied?(client) && upper_bound_satisfied?(client)
      end

      private

      IGNORE_COMMANDS = %w(saslStart saslContinue killCursors)

      def global_client
        @global_client ||= ClientRegistry.instance.global_client('root_authorized').use('admin')
      end

      def events
        @subscriber.started_events.reduce([]) do |evs, e|
          next evs if IGNORE_COMMANDS.include?(e.command_name)

          command = e.command.dup
          if command['aggregate'] && command['pipeline']
            command['pipeline'] = command['pipeline'].map do |stage|
              if stage['$changeStream']
                cs = stage['$changeStream'].dup
                cs.delete('resumeAfter')
                stage.merge('$changeStream' => cs)
              else
                stage
              end
            end
          end

          evs << {
            'command_started_event' => {
              'command' => command,
              'command_name' => e.command_name.to_s,
              'database_name' => e.database_name,
            }
          }
        end
      end

      def server_version(client)
        @server_version ||= client.database.command(buildInfo: 1).first['version']
      end

      def upper_bound_satisfied?(client)
        return true unless @max_server_version
        ClusterConfig.instance.server_version <= @max_server_version
      end

      def lower_bound_satisfied?(client)
        return true unless @min_server_version
        #@min_server_version <= server_version(client)
        @min_server_version <= ClusterConfig.instance.fcv_ish
      end
    end
  end
end
