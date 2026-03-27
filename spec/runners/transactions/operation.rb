# frozen_string_literal: true

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
    class Operation < Mongo::CRUD::Operation
      include RSpec::Matchers

      def needs_session?
        (arguments && arguments['session']) || object =~ /session/
      end

      def execute(target, context)
        op_name = ::Utils.underscore(name).to_sym
        args = if op_name == :with_transaction
                 [ target ]
               else
                 []
               end
        raise "Unknown operation #{name}" if op_name.nil?

        result = send(op_name, target, context, *args)
        if result && result.is_a?(Hash)
          result = result.dup
          result['error'] = false
        end

        result
      rescue Mongo::Error::OperationFailure::Family => e
        raise "OperationFailure had nil result: #{e}" if e.result.nil?

        err_doc = e.result.send(:first_document)
        error_code_name = err_doc['codeName'] || (err_doc['writeConcernError'] && err_doc['writeConcernError']['codeName'])
        if error_code_name.nil?
          # Sometimes the server does not return the error code name,
          # but does return the error code (or we can parse the error code
          # out of the message).
          # https://jira.mongodb.org/browse/SERVER-39706
          warn "Error without error code name: #{e.code}"
        end

        {
          'errorCode' => e.code,
          'errorCodeName' => e.code_name,
          'errorContains' => e.message,
          'errorLabels' => e.labels,
          'exception' => e,
          'error' => true,
        }
      rescue Mongo::Error => e
        {
          'errorContains' => e.message,
          'errorLabels' => e.labels,
          'exception' => e,
          'error' => true,
        }
      rescue bson_error => e
        {
          'exception' => e,
          'clientError' => true,
          'error' => true,
        }
      end

      private

      # operations

      def run_command(database, context)
        # Convert the first key (i.e. the command name) to a symbol.
        cmd = arguments['command'].dup
        command_name = cmd.first.first
        command_value = cmd.delete(command_name)
        cmd = { command_name.to_sym => command_value }.merge(cmd)

        opts = ::Utils.snakeize_hash(transformed_options(context)).dup
        opts[:read] = opts.delete(:read_preference)
        database.command(cmd, opts).documents.first
      end

      def start_transaction(session, _context)
        session.start_transaction(::Utils.convert_operation_options(arguments['options']))
        nil
      end

      def commit_transaction(session, _context)
        session.commit_transaction
        nil
      end

      def abort_transaction(session, _context)
        session.abort_transaction
        nil
      end

      def with_transaction(session, context, _collection)
        unless callback = arguments['callback']
          raise ArgumentError, 'with_transaction requires a callback to be present'
        end

        options = (::Utils.snakeize_hash(arguments['options']) if arguments['options'])
        session.with_transaction(options) do
          callback['operations'].each do |op_spec|
            op = Operation.new(@crud_test, op_spec)
            target = @crud_test.resolve_target(@crud_test.test_client, op)
            rv = op.execute(target, context)
            raise rv['exception'] if rv && rv['exception']
          end
        end
      end

      def assert_session_transaction_state(_collection, context)
        session = context.send(arguments['session'])
        actual_state = session.instance_variable_get(:@state).to_s.sub(/^transaction_|_transaction$/, '').sub(/^no$/,
                                                                                                              'none')
        expect(actual_state).to eq(arguments['state'])
      end

      def targeted_fail_point(_collection, context)
        args = transformed_options(context)
        session = args[:session]
        unless session.pinned_server
          raise ArgumentError, 'Targeted fail point requires session to be pinned to a server'
        end

        client = ClusterTools.instance.direct_client(session.pinned_server.address,
                                                     database: 'admin')
        client.command(arguments['failPoint'])

        $disable_fail_points ||= []
        $disable_fail_points << [
          arguments['failPoint'],
          session.pinned_server.address,
        ]
      end

      def assert_session_pinned(_collection, context)
        args = transformed_options(context)
        session = args[:session]
        return if session.pinned_server

        raise ArgumentError, 'Expected session to be pinned'
      end

      def assert_session_unpinned(_collection, context)
        args = transformed_options(context)
        session = args[:session]
        return unless session.pinned_server

        raise ArgumentError, 'Expected session to not be pinned'
      end

      def wait_for_event(_client, context)
        deadline = Utils.monotonic_time + 5
        loop do
          events = _select_events(context)
          break if events.length >= arguments['count']
          if Utils.monotonic_time >= deadline
            raise "Did not receive an event matching #{arguments} in 5 seconds; received #{events.length} but expected #{arguments['count']} events"
          end

          sleep 0.1
        end
      end

      def assert_event_count(_client, context)
        events = _select_events(context)
        if %w[ServerMarkedUnknownEvent PoolClearedEvent].include?(arguments['event'])
          # We publish SDAM events from both regular and push monitors.
          # This means sometimes there are two ServerMarkedUnknownEvent
          # events published for the same server transition.
          # Allow actual event count to be at least the expected event count
          # in case there are multiple transitions in a single test.
          unless events.length >= arguments['count']
            raise "Expected #{arguments['count']} #{arguments['event']} events, but have #{events.length}"
          end
        else
          unless events.length == arguments['count']
            raise "Expected #{arguments['count']} #{arguments['event']} events, but have #{events.length}"
          end
        end
      end

      def _select_events(context)
        case arguments['event']
        when 'ServerMarkedUnknownEvent'
          context.sdam_subscriber.all_events.select do |event|
            event.is_a?(Mongo::Monitoring::Event::ServerDescriptionChanged) &&
              event.new_description.unknown?
          end
        else
          context.sdam_subscriber.all_events.select do |event|
            event.class.name.sub(/.*::/, '') == arguments['event'].sub(/Event$/, '')
          end
        end
      end

      class ThreadContext
        def initialize
          @operations = Queue.new
          @unexpected_operation_results = []
        end

        def stop?
          !!@stop
        end

        def signal_stop
          @stop = true
        end

        attr_reader :operations, :unexpected_operation_results
      end

      def start_thread(_client, context)
        thread_context = ThreadContext.new
        thread = Thread.new do
          loop do
            begin
              op_spec = thread_context.operations.pop(true)
              op = Operation.new(@crud_test, op_spec)
              target = @crud_test.resolve_target(@crud_test.test_client, op)
              result = op.execute(target, context)
              if op_spec['error']
                thread_context.unexpected_operation_results << result unless result['error']
              elsif result['error']
                thread_context.unexpected_operation_results << result
              end
            rescue ThreadError
              # Queue is empty
            end
            break if thread_context.stop?

            sleep 1
          end
        end
        class << thread
          attr_accessor :context
        end
        thread.context = thread_context
        context.threads ||= {} unless context.threads
        context.threads[arguments['name']] = thread
      end

      def run_on_thread(_client, context)
        thread = context.threads.fetch(arguments['name'])
        thread.context.operations << arguments['operation']
      end

      def wait_for_thread(_client, context)
        thread = context.threads.fetch(arguments['name'])
        thread.context.signal_stop
        thread.join
        return if thread.context.unexpected_operation_results.empty?

        raise "Thread #{arguments['name']} had #{thread.context.unexpected_operation_results}.length unexpected operation results"
      end

      def wait(_client, _context)
        sleep arguments['ms'] / 1000.0
      end

      def record_primary(client, context)
        context.primary_address = client.cluster.next_primary.address
      end

      def run_admin_command(support_client, _context)
        support_client.use('admin').database.command(arguments['command'])
      end

      def wait_for_primary_change(client, context)
        timeout = if arguments['timeoutMS']
                    arguments['timeoutMS'] / 1000.0
                  else
                    10
                  end
        deadline = Utils.monotonic_time + timeout
        loop do
          client.cluster.scan!
          break if client.cluster.next_primary.address != context.primary_address
          raise "Failed to change primary in #{timeout} seconds" if Utils.monotonic_time >= deadline
        end
      end

      # The error to rescue BSON tests for. If we still define
      # BSON::String::IllegalKey then we should rescue that particular error,
      # otherwise, rescue an arbitrary BSON::Error
      def bson_error
        if BSON::String.const_defined?(:IllegalKey)
          BSON::String.const_get(:IllegalKey)
        else
          BSON::Error
        end
      end
    end
  end
end
