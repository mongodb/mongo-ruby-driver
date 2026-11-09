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
        include Mongo::Monitoring::Event::Secure

        # Upper bound of the lsid string cache (see #lsid).
        LSID_CACHE_MAX = 128

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
          @lsid_cache = {}
          @lsid_cache_mutex = Mutex.new
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
        def trace_command(message, _operation_context, connection, &block)
          # Read the command document and name once and pass them on: every
          # read of message.documents allocates.
          doc = message.documents.first
          name = command_name(doc)
          return yield if skip_tracing?(doc, name)

          # Commands should always be nested under their operation span, not directly under
          # the transaction span. Don't pass with_parent to use automatic parent resolution
          # from the currently active span (the operation span).
          span = create_command_span(name, doc, connection)
          # With no SDK and no valid parent, the API returns a span with an
          # invalid context. It cannot be made current, recorded or
          # propagated, so it gets no work at all, not even finishing it.
          return yield unless span.context.valid?

          trace_with_span(span, message, name, doc, &block)
        end

        private

        # Runs the command with the span made current, and finishes the span.
        # A span that is not recording is still made current, so that its
        # context is propagated, but gets no further attributes and no
        # outcome: the SDK would discard them.
        #
        # @param span [ OpenTelemetry::Trace::Span ] a span with a valid context.
        # @param message [ Mongo::Protocol::Message ] the command message.
        # @param name [ String ] the command name.
        # @param doc [ Hash ] the command document.
        #
        # @return [ Object ] the result of the command.
        #
        # rubocop:disable Lint/RescueException
        def trace_with_span(span, message, name, doc)
          recording = span.recording?
          apply_deferred_attributes(span, message, name, doc) if recording
          ::OpenTelemetry::Trace.with_span(span) do |s, _c|
            yield.tap do |result|
              process_command_result(result, s) if recording
            end
          end
        rescue Exception => e
          handle_command_exception(span, e) if recording
          raise e
        ensure
          span.finish
        end
        # rubocop:enable Lint/RescueException

        # Determines whether the command must not be traced. Sensitive auth
        # commands carry credentials in their payloads (SCRAM proofs, cleartext
        # passwords, etc.) and the OpenTelemetry spec requires drivers to skip
        # command spans for them. Hello / legacy hello in all forms
        # (Secure::HELLO_COMMANDS) are also skipped: they are handshake and
        # heartbeat traffic and would only add noise to traces.
        #
        # @param doc [ Hash ] the command document.
        # @param name [ String ] the command name.
        #
        # @return [ Boolean ] true when no command span should be created.
        def skip_tracing?(doc, name)
          return true if HELLO_COMMANDS.include?(name)

          sensitive?(command_name: name, document: doc)
        end

        # Creates a span for a command.
        #
        # @param name [ String ] the command name.
        # @param doc [ Hash ] the command document.
        # @param connection [ Mongo::Server::Connection ] the connection.
        #
        # @return [ OpenTelemetry::Trace::Span ] the created span.
        def create_command_span(name, doc, connection)
          @otel_tracer.start_span(
            name,
            attributes: span_attributes(doc, name, connection),
            kind: :client
          )
        end

        # Processes the command result and updates span attributes.
        #
        # @param result [ Object ] the command result.
        # @param span [ OpenTelemetry::Trace::Span ] the current span.
        def process_command_result(result, span)
          process_cursor_context(result, span)
          maybe_trace_error(result, span)
        end

        # Handles exceptions that occur during command execution.
        #
        # @param span [ OpenTelemetry::Trace::Span ] the span.
        # @param exception [ Exception ] the exception that occurred.
        def handle_command_exception(span, exception)
          if exception.is_a?(Mongo::Error::OperationFailure)
            span.set_attribute('db.response.status_code', exception.code.to_s)
          end
          span.record_exception(exception)
          span.status = ::OpenTelemetry::Trace::Status.error("Unhandled exception of type: #{exception.class}")
        end

        # Builds the attributes passed at span creation: the cheap set the
        # OpenTelemetry spec recommends. They are built for every span,
        # recorded or not, and are the only ones the sampler sees. The rest
        # are set by apply_deferred_attributes once the span is known to
        # record.
        #
        # @param doc [ Hash ] the command document.
        # @param name [ String ] the command name.
        # @param connection [ Mongo::Server::Connection ] the connection.
        #
        # @return [ Hash ] OpenTelemetry span attributes following MongoDB semantic conventions.
        def span_attributes(doc, name, connection)
          {
            'db.system.name' => 'mongodb',
            'db.namespace' => database(doc),
            'db.collection.name' => collection_name(name, doc),
            'db.command.name' => name
          }.compact.merge(connection_attributes(connection))
        end

        # Sets the remaining attributes on a span that is recording. They are
        # not visible to the sampler; only custom samplers keying on them are
        # affected.
        #
        # @param span [ OpenTelemetry::Trace::Span ] the span.
        # @param message [ Mongo::Protocol::Message ] the command message.
        # @param name [ String ] the command name.
        # @param doc [ Hash ] the command document.
        def apply_deferred_attributes(span, message, name, doc)
          {
            'db.query.summary' => query_summary(name, doc),
            'db.query.text' => query_text(message),
            'db.mongodb.cursor_id' => cursor_id(name, doc),
            'db.mongodb.lsid' => lsid(doc),
            'db.mongodb.txn_number' => txn_number(doc)
          }.each do |key, value|
            span.set_attribute(key, value) unless value.nil?
          end
        end

        # Returns connection-related attributes, built once per connection,
        # frozen and kept on the connection. Two threads may build them at
        # the same time; they build the same hash, so either result is fine.
        #
        # @param connection [ Mongo::Server::Connection ] the connection.
        #
        # @return [ Hash ] connection span attributes.
        def connection_attributes(connection)
          connection.tracing_attributes ||= {
            'server.port' => connection.address.port,
            'server.address' => connection.address.host,
            'network.transport' => connection.transport.to_s,
            'db.mongodb.server_connection_id' => connection.description.server_connection_id,
            'db.mongodb.driver_connection_id' => connection.id
          }.freeze
        end

        # Processes cursor context from the command result.
        #
        # @param result [ Object ] the command result.
        # @param span [ OpenTelemetry::Trace::Span ] the current span.
        def process_cursor_context(result, span)
          cursor_id = normalize_cursor_id(result.cursor_id)
          return unless cursor_id.positive?

          span.set_attribute('db.mongodb.cursor_id', cursor_id)
        end

        # Normalizes a cursor id to a plain Integer.
        #
        # OP_MSG replies deserialized in :bson mode expose the cursor id as a
        # BSON::Int64, which does not implement Numeric#positive? and is not a
        # valid OpenTelemetry attribute type.
        #
        # @param cursor_id [ Integer | BSON::Int64 ] the raw cursor id.
        #
        # @return [ Integer ] the cursor id as an Integer.
        def normalize_cursor_id(cursor_id)
          cursor_id.is_a?(BSON::Int64) ? cursor_id.value : cursor_id
        end

        # Records error status code if the command failed.
        #
        # @param result [ Object ] the command result.
        # @param span [ OpenTelemetry::Trace::Span ] the current span.
        def maybe_trace_error(result, span)
          return if result.successful?

          span.set_attribute('db.response.status_code', result.error.code.to_s)
          begin
            result.validate!
          rescue Mongo::Error::OperationFailure => e
            span.record_exception(e)
          end
        end

        # Generates a summary string for the query.
        #
        # @param name [ String ] the command name.
        # @param doc [ Hash ] the command document.
        #
        # @return [ String ] summary in format "command_name db.collection" or "command_name db".
        def query_summary(name, doc)
          if (coll_name = collection_name(name, doc))
            "#{name} #{database(doc)}.#{coll_name}"
          else
            "#{name} #{database(doc)}"
          end
        end

        # Extracts the collection name from the command document.
        #
        # @param name [ String ] the command name.
        # @param doc [ Hash ] the command document.
        #
        # @return [ String | nil ] the collection name, or nil if not applicable.
        def collection_name(name, doc)
          case name
          when 'getMore'
            doc['collection'].to_s
          when 'listCollections', 'listDatabases', 'commitTransaction', 'abortTransaction'
            nil
          else
            # The first value, read without building an array of all of them.
            value = doc.each_value { |v| break v } # rubocop:disable Lint/UnreachableLoop -- first value only
            # Return nil if the value is not a string (e.g., for admin commands that have numeric values)
            value.is_a?(String) ? value : nil
          end
        end

        # Extracts the command name, the first key of the command document,
        # without building an array of all keys.
        #
        # @param doc [ Hash ] the command document.
        #
        # @return [ String ] the command name.
        def command_name(doc)
          doc.each_key { |key| return key.to_s } # rubocop:disable Lint/UnreachableLoop -- first key only
          ''
        end

        # Extracts the database name from the command document.
        #
        # @param doc [ Hash ] the command document.
        #
        # @return [ String ] the database name.
        def database(doc)
          doc['$db'].to_s
        end

        # Checks if query text capture is enabled.
        #
        # @return [ Boolean ] true if query text should be captured.
        def query_text?
          @query_text_max_length.positive?
        end

        # Extracts the cursor ID from getMore commands.
        #
        # @param name [ String ] the command name.
        # @param doc [ Hash ] the command document.
        #
        # @return [ Integer | nil ] the cursor ID, or nil if not a getMore command.
        def cursor_id(name, doc)
          return unless name == 'getMore'

          doc['getMore'].value
        end

        # Extracts the logical session ID from the command, as a UUID string.
        # The string is formatted once per session id and kept in a small
        # cache, cleared when it reaches LSID_CACHE_MAX entries.
        #
        # @param doc [ Hash ] the command document.
        #
        # @return [ String | nil ] the session ID, or nil if not present.
        def lsid(doc)
          lsid_doc = doc['lsid']
          return unless lsid_doc

          binary = lsid_doc['id']
          key = binary.data
          cached = @lsid_cache_mutex.synchronize { @lsid_cache[key] }
          return cached if cached

          uuid = binary.to_uuid
          @lsid_cache_mutex.synchronize do
            @lsid_cache.clear if @lsid_cache.size >= LSID_CACHE_MAX
            @lsid_cache[key] = uuid
          end
          uuid
        end

        # Extracts the transaction number from the command.
        #
        # @param doc [ Hash ] the command document.
        #
        # @return [ Integer | nil ] the transaction number, or nil if not present.
        def txn_number(doc)
          txn_num = doc['txnNumber']
          return unless txn_num

          txn_num.value
        end

        # Keys to exclude from query text capture.
        EXCLUDED_KEYS = %w[lsid $db $clusterTime signature].freeze

        # Ellipsis for truncated query text.
        ELLIPSIS = '...'

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
            "#{text[0...@query_text_max_length]}#{ELLIPSIS}"
          else
            text
          end
        end
      end
    end
  end
end
