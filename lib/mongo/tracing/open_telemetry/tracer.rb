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
        #   Defaults to nil, which means it will check the environment variable
        #   OTEL_RUBY_INSTRUMENTATION_MONGODB_ENABLED (values: true/1/yes). If the
        #   environment variable is not set, OpenTelemetry will be disabled by default.
        # @param query_text_max_length [ Integer | nil ] maximum length for captured query text.
        #   Defaults to nil, which means it will check the environment variable
        #   OTEL_RUBY_INSTRUMENTATION_MONGODB_QUERY_TEXT_MAX_LENGTH. If the environment variable is not set,
        #   the query text will not be captured.
        # @param otel_tracer [ OpenTelemetry::Trace::Tracer | nil ] the OpenTelemetry tracer
        #   implementation to use. Defaults to nil, which means it will use the default tracer
        #   from OpenTelemetry's tracer provider.
        def initialize(enabled: nil, query_text_max_length: nil, otel_tracer: nil)
          @enabled = if enabled.nil?
                       %w[true 1 yes].include?(ENV['OTEL_RUBY_INSTRUMENTATION_MONGODB_ENABLED']&.downcase)
                     else
                       enabled
                     end
          check_opentelemetry_loaded
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
        # @return [Boolean] true if OpenTelemetry is enabled, false otherwise.
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

        # Start a transaction span and activate its context.
        #
        # @param session [Mongo::Session] The session starting the transaction.
        def start_transaction_span(session)
          return unless enabled?

          key = transaction_map_key(session)
          return unless key

          # Create the transaction span with minimal attributes
          span = @otel_tracer.start_span(
            'transaction',
            attributes: { 'db.system.name' => 'mongodb' },
            kind: :client
          )

          # Create a context containing this span
          context = ::OpenTelemetry::Trace.context_with_span(span)

          # Activate the context and store the token for later detachment
          token = ::OpenTelemetry::Context.attach(context)

          # Store span, token, and context for later retrieval
          transaction_span_map[key] = span
          transaction_token_map[key] = token
          transaction_context_map[key] = context
        end

        # Finish a transaction span and deactivate its context.
        #
        # @param session [Mongo::Session] The session finishing the transaction.
        def finish_transaction_span(session)
          return unless enabled?

          key = transaction_map_key(session)
          return unless key

          span = transaction_span_map.delete(key)
          token = transaction_token_map.delete(key)
          transaction_context_map.delete(key)

          return unless span && token

          begin
            span.finish
          ensure
            ::OpenTelemetry::Context.detach(token)
          end
        end

        # Returns the cursor context map for tracking cursor-related OpenTelemetry contexts.
        #
        # @return [ Hash ] map of cursor IDs to OpenTelemetry contexts.
        def cursor_context_map
          @cursor_context_map ||= {}
        end

        # Generates a unique key for cursor tracking in the context map.
        #
        # @param session [ Mongo::Session ] the session associated with the cursor.
        # @param cursor_id [ Integer ] the cursor ID.
        #
        # @return [ String | nil ] unique key combining session ID and cursor ID, or nil if either is nil.
        def cursor_map_key(session, cursor_id)
          return if cursor_id.nil? || session.nil?

          "#{session.session_id['id'].to_uuid}-#{cursor_id}"
        end

        # Determines the parent OpenTelemetry context for an operation.
        #
        # Returns the transaction context if the operation is part of a transaction,
        # otherwise returns nil. Cursor-based context nesting is not currently implemented.
        #
        # @param operation_context [ Mongo::Operation::Context ] the operation context.
        # @param cursor_id [ Integer ] the cursor ID, if applicable.
        #
        # @return [ OpenTelemetry::Context | nil ] parent context or nil.
        def parent_context_for(operation_context, cursor_id)
          if (key = transaction_map_key(operation_context.session))
            transaction_context_map[key]
          elsif (_key = cursor_map_key(operation_context.session, cursor_id))
            # We return nil here unless we decide how to nest cursor operations.
            nil
          end
        end

        # Returns the transaction context map for tracking active transaction contexts.
        #
        # @return [ Hash ] map of transaction keys to OpenTelemetry contexts.
        def transaction_context_map
          @transaction_context_map ||= {}
        end

        # Returns the transaction span map for tracking active transaction spans.
        #
        # @return [ Hash ] map of transaction keys to OpenTelemetry spans.
        def transaction_span_map
          @transaction_span_map ||= {}
        end

        # Returns the transaction token map for tracking context attachment tokens.
        #
        # @return [ Hash ] map of transaction keys to OpenTelemetry context tokens.
        def transaction_token_map
          @transaction_token_map ||= {}
        end

        # Generates a unique key for transaction tracking.
        #
        # Returns nil for implicit sessions or sessions not in a transaction.
        #
        # @param session [ Mongo::Session ] the session.
        #
        # @return [ String | nil ] unique key combining session ID and transaction number, or nil.
        def transaction_map_key(session)
          return if session.nil? || session.implicit? || !session.in_transaction?

          "#{session.session_id['id'].to_uuid}-#{session.txn_num}"
        end

        private

        def check_opentelemetry_loaded
          return unless @enabled
          return if defined?(::OpenTelemetry)

          Logger.logger.warn('OpenTelemetry tracing for MongoDB is enabled, ' \
                             'but the OpenTelemetry library is not loaded. ' \
                             'Disabling tracing.')
          @enabled = false
        end

        def initialize_tracer
          return unless enabled?

          ::OpenTelemetry.tracer_provider.tracer(
            'mongo-ruby-driver',
            Mongo::VERSION
          )
        end
      end
    end
  end
end
