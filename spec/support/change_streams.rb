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

RSpec::Matchers.define :match_result do |test|
  match do |actual|
    test.match_result?(actual)
  end
end

RSpec::Matchers.define :match_commands do |test|
  match do |actual|
    test.match_commands?(actual)
  end
end

require 'support/change_streams/operation'

module Mongo
  module ChangeStreams
    class Spec

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
        @spec_tests = @spec['tests']
        @coll1 = @spec['collection_name']
        @coll2 = @spec['collection2_name']
        @db1 = @spec['database_name']
        @db2 = @spec['database2_name']
      end

      # Get a list of ChangeStreamsTests for each test definition.
      #
      # @example Get the list of ChangeStreamsTests.
      #   spec.tests
      #
      # @return [ Array<ChangeStreamsTest> ] The list of ChangeStreamsTests.
      #
      # @since 2.0.0
      def tests
        @spec_tests.map do |test|
          ChangeStreamsTest.new(test, @coll1, @coll2, @db1, @db2)
        end
      end

      class ChangeStreamsTest
        # The test description.
        #
        # @return [ String ] description The test description.
        #
        # @since 2.0.0
        attr_reader :description

        def initialize(test, coll1, coll2, db1, db2)
          @description = test['description']
          @min_server_version = test['minServerVersion']
          @max_server_version = test['maxServerVersion']
          @target_type = test['target']
          @topologies = test['topology'].map do |topology|
            {'single' => :single, 'replicaset' => :replica_set, 'sharded' => :sharded}[topology]
          end
          @pipeline = test['changeStreamPipeline'] || []
          @options = test['changeStreamOptions'] || {}
          @operations = test['operations'].map { |op| Operation.new(op) }
          @expectations = test['expectations']
          @result = test['result']
          @coll1_name = coll1
          @coll2_name = coll2
          @db1_name = db1
          @db2_name = db2
        end

        attr_reader :topologies

        def setup_test
          @global_client = ClientRegistry.instance.global_client('root_authorized').use('admin')

          @db1 = @global_client.use(@db1_name).database.tap(&:drop)
          @db2 = @global_client.use(@db2_name).database.tap(&:drop)

          @db1[@coll1_name].create
          @db2[@coll2_name].create

          client = ClientRegistry.instance.global_client('root_authorized').with(
            database: @db1_name,
            app_name: 'this is used solely to force the new client to create its own cluster')
          client.subscribe(Mongo::Monitoring::COMMAND, EventSubscriber.clear_events!)

          @target = case @target_type
                   when 'client'
                     client
                   when 'database'
                     client.database
                   when 'collection'
                     client[@coll1_name]
                   end
        end

        def run
          change_stream = begin
            @target.watch(@pipeline, @options)
          rescue Mongo::Error::OperationFailure => e
            return {
              result: { 'error' => { 'code' => e.code } },
              events: events
            }
          end

          @operations.each do |op|
            op.execute(@db1, @db2)
          end

          changes = [].tap do |changes|
                           next unless @result['success']

                           unless @result['success'].empty?
                             change_stream.take_while do |change|
                               changes << change
                               changes.length < @result['success'].length
                             end
                           end

                           change_stream.close
                         end

          {
            result: { 'success' => changes },
            events: events,
          }
        end

        def match_result?(result)
          case @result.first.first
          when 'success'
            match_success?(result)
          when 'error'
            @result == result
          end
        end

        def match_commands?(actual)
          @expectations.each_with_index.all? do |e, i|
            actual[i] && match?(e, actual[i])
          end
        end

        def server_version_satisfied?(client)
          lower_bound_satisfied?(client) && upper_bound_satisfied?(client)
        end

        private

        def events
          EventSubscriber.started_events.reduce([]) do |evs, e|
            next evs if %w(saslStart saslContinue killCursors).include?(e.command_name)

            evs << {
              'command_started_event' => {
                'command' => e.command,
                'command_name' => e.command_name.to_s,
                'database_name' => e.database_name
              }
            }
          end
        end

        def match_success?(result)
          return false unless result['success']
          match?(@result['success'], result['success'])
        end

        def match?(expected, actual)
          return !!actual if expected.to_s == '42'
          return match_array?(expected, actual) if expected.is_a?(Array)
          return match_hash?(expected, actual) if expected.is_a?(Hash)
          expected == actual
        end

        def match_array?(expected, actual)
          return false unless actual.is_a?(Array)

          expected.each_with_index.all? do |e, i|
            actual[i] && match?(e, actual[i])
          end
        end

        def match_hash?(expected, actual)
          return false unless actual.is_a?(Hash)

          expected.all? do |k, v|
            if v.is_a?(Hash) && !v.empty?
              case v.first.first
              when '$numberInt'
                v = v.first.last.to_i
              end
            end

            actual[k] && match?(v, actual[k])
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
end
