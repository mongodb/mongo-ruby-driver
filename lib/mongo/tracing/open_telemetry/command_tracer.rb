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
      # CommandTracer is responsible for tracing MongoDB server commands using OpenTelemetry.
      #
      # @api private
      class CommandTracer
        # Initializes a new CommandTracer.
        #
        # @param otel_tracer [ OpenTelemetry::Trace::Tracer ] the OpenTelemetry tracer.
        # @param parent_tracer [ Mongo::Tracing::OpenTelemetry::Tracer ] the parent tracer
        #   for accessing shared context maps.
        # @param query_text_max_length [ Integer ] maximum length for captured query text.
        #   Defaults to 0 (no query text capture).
        def initialize(otel_tracer, parent_tracer, query_text_max_length: 0)
          @otel_tracer = otel_tracer
          @parent_tracer = parent_tracer
          @query_text_max_length = query_text_max_length
        end

        # Starts a span for a MongoDB command.
        #
        # @param message [ Mongo::Protocol::Message ] the command message.
        # @param operation_context [ Mongo::Operation::Context ] the operation context.
        # @param connection [ Mongo::Server::Connection ] the connection.
        def start_span(message, operation_context, connection); end

        # Trace a MongoDB command.
        #
        # Creates an OpenTelemetry span for the command, capturing attributes such as
        # command name, database name, collection name, server address, connection IDs,
        # and optionally query text. The span is automatically nested under the current
        # operation span and is finished when the command completes or fails.
        #
        # @param message [ Mongo::Protocol::Message ] the command message to trace.
        # @param _operation_context [ Mongo::Operation::Context ] the context of the operation.
        # @param connection [ Mongo::Server::Connection ] the connection used to send the command.
        #
        # @yield the block representing the command to be traced.
        #
        # @return [ Object ] the result of the command.
        # rubocop:disable Lint/RescueException
        def trace_command(message, _operation_context, connection)
          # Commands should always be nested under their operation span, not directly under
          # the transaction span. Don't pass with_parent to use automatic parent resolution
          # from the currently active span (the operation span).
          span = create_command_span(message, connection)
          ::OpenTelemetry::Trace.with_span(span) do |s, c|
            yield.tap do |result|
              process_command_result(result, cursor_id(message), c, s)
            end
          end
        rescue Exception => e
          handle_command_exception(span, e)
          raise e
        ensure
          span&.finish
        end
        # rubocop:enable Lint/RescueException

        private

        # Creates a span for a command.
        #
        # @param message [ Mongo::Protocol::Message ] the command message.
        # @param connection [ Mongo::Server::Connection ] the connection.
        #
        # @return [ OpenTelemetry::Trace::Span ] the created span.
        def create_command_span(message, connection)
          @otel_tracer.start_span(
            command_name(message),
            attributes: span_attributes(message, connection),
            kind: :client
          )
        end

        # Processes the command result and updates span attributes.
        #
        # @param result [ Object ] the command result.
        # @param cursor_id [ Integer | nil ] the cursor ID.
        # @param context [ OpenTelemetry::Context ] the context.
        # @param span [ OpenTelemetry::Trace::Span ] the current span.
        def process_command_result(result, cursor_id, context, span)
          process_cursor_context(result, cursor_id, context, span)
          maybe_trace_error(result, span)
        end

        # Handles exceptions that occur during command execution.
        #
        # @param span [ OpenTelemetry::Trace::Span | nil ] the span.
        # @param exception [ Exception ] the exception that occurred.
        def handle_command_exception(span, exception)
          return unless span

          if exception.is_a?(Mongo::Error::OperationFailure)
            span.set_attribute('db.response.status_code', exception.code.to_s)
          end
          span.record_exception(exception)
          span.status = ::OpenTelemetry::Trace::Status.error("Unhandled exception of type: #{exception.class}")
        end

        # Builds span attributes for the command.
        #
        # @param message [ Mongo::Protocol::Message ] the command message.
        # @param connection [ Mongo::Server::Connection ] the connection.
        #
        # @return [ Hash ] OpenTelemetry span attributes following MongoDB semantic conventions.
        def span_attributes(message, connection)
          base_attributes(message)
            .merge(connection_attributes(connection))
            .merge(session_attributes(message))
            .compact
        end

        # Returns base database and command attributes.
        #
        # @param message [ Mongo::Protocol::Message ] the command message.
        #
        # @return [ Hash ] base span attributes.
        def base_attributes(message)
          {
            'db.system' => 'mongodb',
            'db.namespace' => database(message),
            'db.collection.name' => collection_name(message),
            'db.command.name' => command_name(message),
            'db.query.summary' => query_summary(message),
            'db.query.text' => query_text(message)
          }
        end

        # Returns connection-related attributes.
        #
        # @param connection [ Mongo::Server::Connection ] the connection.
        #
        # @return [ Hash ] connection span attributes.
        def connection_attributes(connection)
          {
            'server.port' => connection.address.port,
            'server.address' => connection.address.host,
            'network.transport' => connection.transport.to_s,
            'db.mongodb.server_connection_id' => connection.server.description.server_connection_id,
            'db.mongodb.driver_connection_id' => connection.id
          }
        end

        # Returns session and transaction attributes.
        #
        # @param message [ Mongo::Protocol::Message ] the command message.
        #
        # @return [ Hash ] session span attributes.
        def session_attributes(message)
          {
            'db.mongodb.cursor_id' => cursor_id(message),
            'db.mongodb.lsid' => lsid(message),
            'db.mongodb.txn_number' => txn_number(message)
          }
        end

        # Processes cursor context from the command result.
        #
        # @param result [ Object ] the command result.
        # @param _cursor_id [ Integer | nil ] the cursor ID (unused).
        # @param _context [ OpenTelemetry::Context ] the context (unused).
        # @param span [ OpenTelemetry::Trace::Span ] the current span.
        def process_cursor_context(result, _cursor_id, _context, span)
          return unless result.has_cursor_id? && result.cursor_id.positive?

          span.set_attribute('db.mongodb.cursor_id', result.cursor_id)
        end

        # Records error status code if the command failed.
        #
        # @param result [ Object ] the command result.
        # @param span [ OpenTelemetry::Trace::Span ] the current span.
        def maybe_trace_error(result, span)
          return if result.successful?

          span.set_attribute('db.response.status_code', result.error.code.to_s)
        end

        # Generates a summary string for the query.
        #
        # @param message [ Mongo::Protocol::Message ] the command message.
        #
        # @return [ String ] summary in format "command_name db.collection" or "command_name db".
        def query_summary(message)
          if (coll_name = collection_name(message))
            "#{command_name(message)} #{database(message)}.#{coll_name}"
          else
            "#{command_name(message)} #{database(message)}"
          end
        end

        # Extracts the collection name from the command message.
        #
        # @param message [ Mongo::Protocol::Message ] the command message.
        #
        # @return [ String | nil ] the collection name, or nil if not applicable.
        def collection_name(message)
          case message.documents.first.keys.first
          when 'getMore'
            message.documents.first['collection'].to_s
          when 'listCollections', 'listDatabases', 'commitTransaction', 'abortTransaction'
            nil
          else
            value = message.documents.first.values.first
            # Return nil if the value is not a string (e.g., for admin commands that have numeric values)
            value.is_a?(String) ? value : nil
          end
        end

        # Extracts the command name from the message.
        #
        # @param message [ Mongo::Protocol::Message ] the command message.
        #
        # @return [ String ] the command name.
        def command_name(message)
          message.documents.first.keys.first.to_s
        end

        # Extracts the database name from the message.
        #
        # @param message [ Mongo::Protocol::Message ] the command message.
        #
        # @return [ String ] the database name.
        def database(message)
          message.documents.first['$db'].to_s
        end

        # Checks if query text capture is enabled.
        #
        # @return [ Boolean ] true if query text should be captured.
        def query_text?
          @query_text_max_length.positive?
        end

        # Extracts the cursor ID from getMore commands.
        #
        # @param message [ Mongo::Protocol::Message ] the command message.
        #
        # @return [ Integer | nil ] the cursor ID, or nil if not a getMore command.
        def cursor_id(message)
          return unless command_name(message) == 'getMore'

          message.documents.first['getMore'].value
        end

        # Extracts the logical session ID from the command.
        #
        # @param message [ Mongo::Protocol::Message ] the command message.
        #
        # @return [ BSON::Binary | nil ] the session ID, or nil if not present.
        def lsid(message)
          lsid_doc = message.documents.first['lsid']
          return unless lsid_doc

          lsid_doc['id'].to_uuid
        end

        # Extracts the transaction number from the command.
        #
        # @param message [ Mongo::Protocol::Message ] the command message.
        #
        # @return [ Integer | nil ] the transaction number, or nil if not present.
        def txn_number(message)
          txn_num = message.documents.first['txnNumber']
          return unless txn_num

          txn_num.value
        end

        # Keys to exclude from query text capture.
        EXCLUDED_KEYS = %w[lsid $db $clusterTime signature].freeze

        # Ellipsis for truncated query text.
        ELLIPSES = '...'

        # Extracts and formats the query text from the command.
        #
        # @param message [ Mongo::Protocol::Message ] the command message.
        #
        # @return [ String | nil ] JSON representation of the command, truncated if necessary, or nil if disabled.
        def query_text(message)
          return unless query_text?

          text = message
                 .payload['command']
                 .reject { |key, _| EXCLUDED_KEYS.include?(key) }
                 .to_json
          if text.length > @query_text_max_length
            "#{text[0...@query_text_max_length]}#{ELLIPSES}"
          else
            text
          end
        end
      end
    end
  end
end
