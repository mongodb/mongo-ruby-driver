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

        # Commands for which a span MUST NOT be created. The OpenTelemetry spec
        # requires drivers to skip command spans for sensitive commands listed in
        # the Command Logging and Monitoring spec. We additionally skip hello /
        # legacy hello in all forms — these are handshake/heartbeat traffic and
        # would only add noise to traces.
        HELLO_COMMANDS = %w[hello ismaster isMaster].freeze

        # Upper bound of the lsid UUID memo cache (see #lsid).
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
        # rubocop:disable Lint/RescueException
        def trace_command(message, _operation_context, connection)
          # The command document and name are extracted once and threaded
          # through: the extraction helpers below allocate on every call.
          doc = message.documents.first
          name = command_name(doc)
          return yield if skip_tracing?(doc, name)

          # Commands should always be nested under their operation span, not directly under
          # the transaction span. Don't pass with_parent to use automatic parent resolution
          # from the currently active span (the operation span).
          span = create_command_span(name, doc, connection)
          # An invalid context has no trace identity: it cannot be propagated,
          # continued, or correlated with anything downstream, so every
          # operation on it is waste. This is a state check on the span we
          # were handed, not detection of whether the SDK is available — a
          # custom API-only provider returning real spans sees the full path.
          # Must not key on recording?: an unsampled-but-valid context still
          # has to be made current for propagation.
          return yield unless span.context.valid?

          cursor = cursor_id(name, doc)
          apply_deferred_attributes(span, message, name, doc, cursor) if span.recording?
          ::OpenTelemetry::Trace.with_span(span) do |s, c|
            yield.tap do |result|
              process_command_result(result, cursor, c, s)
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

        # Determines whether the command must not be traced. Sensitive auth
        # commands carry credentials in their payloads (SCRAM proofs, cleartext
        # passwords, etc.) and the OpenTelemetry spec requires drivers to skip
        # command spans for them. Hello / legacy hello are also skipped to keep
        # handshake traffic out of traces.
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

        # Builds the attributes passed at span creation: the cheap,
        # sampler-plausible set. Expensive attributes are deferred to
        # apply_deferred_attributes — building them here would defeat the
        # sampler's purpose, and on a non-recording span they would be
        # discarded anyway. Keys whose value is nil are omitted rather than
        # compacted afterwards, so the common case allocates nothing extra.
        #
        # @param doc [ Hash ] the command document.
        # @param name [ String ] the command name.
        # @param connection [ Mongo::Server::Connection ] the connection.
        #
        # @return [ Hash ] OpenTelemetry span attributes following MongoDB semantic conventions.
        def span_attributes(doc, name, connection)
          attrs = {
            'db.system.name' => 'mongodb',
            'db.namespace' => database(doc),
            'db.command.name' => name
          }
          if (coll_name = collection_name(name, doc))
            attrs['db.collection.name'] = coll_name
          end
          attrs.merge(connection_attributes(connection))
        end

        # Returns connection-related attributes, computed once per connection
        # and frozen. Setting the ivar from here is a benign race: competing
        # threads build identical frozen hashes. The value dies with the
        # connection, so no cleanup is needed.
        #
        # @param connection [ Mongo::Server::Connection ] the connection.
        #
        # @return [ Hash ] connection span attributes.
        def connection_attributes(connection)
          attrs = connection.instance_variable_get(:@otel_connection_attributes)
          unless attrs
            attrs = {
              'server.port' => connection.address.port,
              'server.address' => connection.address.host,
              'network.transport' => connection.transport.to_s,
              'db.mongodb.server_connection_id' => connection.description.server_connection_id,
              'db.mongodb.driver_connection_id' => connection.id
            }.freeze
            connection.instance_variable_set(:@otel_connection_attributes, attrs)
          end
          attrs
        end

        # Sets the expensive attributes after span creation. Only called for
        # recording spans: on non-recording spans set_attribute discards.
        # Note for reviewers: these attributes are invisible to the sampler,
        # which only sees what is passed to start_span. The built-in samplers
        # do not read attributes, and db.query.text is off by default.
        #
        # @param span [ OpenTelemetry::Trace::Span ] the current span.
        # @param message [ Mongo::Protocol::Message ] the command message.
        # @param name [ String ] the command name.
        # @param doc [ Hash ] the command document.
        # @param cursor [ Integer | nil ] the cursor id, extracted once per command.
        def apply_deferred_attributes(span, message, name, doc, cursor)
          span.set_attribute('db.query.summary', query_summary(name, doc))
          if (text = query_text(message))
            span.set_attribute('db.query.text', text)
          end
          if (lsid_value = lsid(doc))
            span.set_attribute('db.mongodb.lsid', lsid_value)
          end
          unless cursor.nil?
            span.set_attribute('db.mongodb.cursor_id', cursor)
          end
          if (txn = txn_number(doc))
            span.set_attribute('db.mongodb.txn_number', txn)
          end
        end

        # Processes cursor context from the command result.
        #
        # @param result [ Object ] the command result.
        # @param _cursor_id [ Integer | nil ] the cursor ID (unused).
        # @param _context [ OpenTelemetry::Context ] the context (unused).
        # @param span [ OpenTelemetry::Trace::Span ] the current span.
        def process_cursor_context(result, _cursor_id, _context, span)
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
            # Iterate instead of using doc.values.first: the block form is
            # allocation-free (see #command_name).
            value = nil
            # rubocop:disable Lint/UnreachableLoop -- intentional: only the first entry is needed
            doc.each_value do |v|
              value = v
              break
            end
            # rubocop:enable Lint/UnreachableLoop
            # Return nil if the value is not a string (e.g., for admin commands that have numeric values)
            value.is_a?(String) ? value : nil
          end
        end

        # Extracts the command name from the command document. Iterates
        # instead of using doc.keys.first: the block form is allocation-free,
        # while keys builds an array of every top-level key per call — and
        # this runs on every traced command.
        #
        # @param doc [ Hash ] the command document.
        #
        # @return [ String ] the command name.
        def command_name(doc)
          # rubocop:disable Lint/UnreachableLoop -- intentional: only the first entry is needed
          doc.each_key { |key| return key.to_s }
          # rubocop:enable Lint/UnreachableLoop
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

        # Extracts the logical session ID from the command. The UUID string
        # is formatted once per session id and memoized in a bounded cache:
        # the value is invariant for the life of a session, and formatting it
        # per command showed up in the DRIVERS-3620 profile.
        #
        # @param doc [ Hash ] the command document.
        #
        # @return [ String | nil ] the session ID as a UUID string, or nil if not present.
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
