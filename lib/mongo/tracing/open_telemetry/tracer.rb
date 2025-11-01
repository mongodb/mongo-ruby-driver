# frozen_string_literal: true

# Copyright (C) 2025-present MongoDB Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  module Tracing
    module OpenTelemetry
      # OpenTelemetry tracer for MongoDB operations and commands.
      # @api private
      class Tracer
        # @return [ OpenTelemetry::Trace::Tracer ] the OpenTelemetry tracer implementation
        #   used to create spans for MongoDB operations and commands.
        #
        # @api private
        attr_reader :otel_tracer

        # Initializes a new OpenTelemetry tracer.
        #
        # @param enabled [ Boolean | nil ] whether OpenTelemetry is enabled or not.
        #   If nil, it will check the environment variable
        #   OTEL_RUBY_INSTRUMENTATION_MONGODB_ENABLED.
        # @param otel_tracer [ OpenTelemetry::Trace::Tracer | nil ] the OpenTelemetry tracer
        #   implementation to use. If nil, it will use the default tracer from
        #   OpenTelemetry's tracer provider.
        def initialize(enabled: nil, query_text_max_length: nil, otel_tracer: nil)
          @enabled = if enabled.nil?
                       %w[true 1 yes].include?(ENV['OTEL_RUBY_INSTRUMENTATION_MONGODB_ENABLED']&.downcase)
                     else
                       enabled
                     end
          @query_text_max_length = if query_text_max_length.nil?
                                     ENV['OTEL_RUBY_INSTRUMENTATION_MONGODB_QUERY_TEXT_MAX_LENGTH'].to_i
                                   else
                                     query_text_max_length
                                   end
          @otel_tracer = otel_tracer || initialize_tracer
          @operation_tracer = OperationTracer.new(@otel_tracer, self)
          @command_tracer = CommandTracer.new(@otel_tracer, self, query_text_max_length: @query_text_max_length)
        end

        # Whether OpenTelemetry is enabled or not.
        #
        # # @return [Boolean] true if OpenTelemetry is enabled, false otherwise.
        def enabled?
          @enabled
        end

        # Trace a MongoDB operation.
        #
        # @param operation [Mongo::Operation] The MongoDB operation to trace.
        # @param operation_context [Mongo::Operation::Context] The context of the operation.
        # @param op_name [String, nil] An optional name for the operation.
        # @yield The block representing the operation to be traced.
        # @return [Object] The result of the operation.
        def trace_operation(operation, operation_context, op_name: nil, &block)
          return yield unless enabled?

          @operation_tracer.trace_operation(operation, operation_context, op_name: op_name, &block)
        end

        # Trace a MongoDB command.
        #
        # @param message [Mongo::Protocol::Message] The MongoDB command message to trace.
        # @param operation_context [Mongo::Operation::Context] The context of the operation.
        # @param connection [Mongo::Server::Connection] The connection used to send the command
        # @yield The block representing the command to be traced.
        # @return [Object] The result of the command.
        def trace_command(message, operation_context, connection, &block)
          return yield unless enabled?

          @command_tracer.trace_command(message, operation_context, connection, &block)
        end

        def cursor_context_map
          @cursor_context_map ||= {}
        end

        def cursor_map_key(session, cursor_id)
          return if cursor_id.nil? || session.nil?

          "#{session.session_id['id'].to_uuid}-#{cursor_id}"
        end

        def parent_context_for(operation_context, cursor_id)
          if (key = transaction_map_key(operation_context.session))
            transaction_context_map[key]
          elsif (_key = cursor_map_key(operation_context.session, cursor_id))
            # We return nil here unless we decide how to nest cursor operations.
            nil
          end
        end

        def transaction_context_map
          @transaction_context_map ||= {}
        end

        def transaction_map_key(session)
          return if session.nil? || session.implicit? || !session.in_transaction?

          "#{session.session_id['id'].to_uuid}-#{session.txn_num}"
        end

        private

        def initialize_tracer
          if enabled?
            # Obtain the proper tracer from OpenTelemetry's tracer provider.
            ::OpenTelemetry.tracer_provider.tracer(
              'mongo-ruby-driver',
              Mongo::VERSION
            )
          else
            # No-op tracer when OpenTelemetry is not enabled.
            ::OpenTelemetry::Trace::Tracer.new
          end
        end
      end
    end
  end
end
