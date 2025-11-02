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
        def initialize(otel_tracer, parent_tracer, query_text_max_length: 0)
          @otel_tracer = otel_tracer
          @parent_tracer = parent_tracer
          @query_text_max_length = query_text_max_length
        end

        def start_span(message, operation_context, connection); end

        def trace_command(message, operation_context, connection)
          # Commands should always be nested under their operation span, not directly under
          # the transaction span. Don't pass with_parent to use automatic parent resolution
          # from the currently active span (the operation span).
          span = @otel_tracer.start_span(
            command_name(message),
            attributes: span_attributes(message, connection),
            kind: :client
          )
          ::OpenTelemetry::Trace.with_span(span) do |s, c|
            yield.tap do |result|
              process_cursor_context(result, cursor_id(message), c, s)
              maybe_trace_error(result, s)
            end
          end
        rescue Exception => e
          span&.set_attribute('db.response.status_code', e.code.to_s) if e.is_a?(Mongo::Error::OperationFailure)
          span&.record_exception(e)
          span&.status = ::OpenTelemetry::Trace::Status.error("Unhandled exception of type: #{e.class}")
          raise e
        ensure
          span&.finish
        end

        private

        def span_attributes(message, connection)
          {
            'db.system' => 'mongodb',
            'db.namespace' => database(message),
            'db.collection.name' => collection_name(message),
            'db.command.name' => command_name(message),
            'db.query.summary' => query_summary(message),
            'server.port' => connection.address.port,
            'server.address' => connection.address.host,
            'network.transport' => connection.transport.to_s,
            'db.mongodb.server_connection_id' => connection.server.description.server_connection_id,
            'db.mongodb.driver_connection_id' => connection.id,
            'db.mongodb.cursor_id' => cursor_id(message),
            'db.mongodb.lsid' => lsid(message),
            'db.mongodb.txn_number' => txn_number(message),
            'db.query.text' => query_text(message)
          }.compact
        end

        def process_cursor_context(result, _cursor_id, _context, span)
          return unless result.has_cursor_id? && result.cursor_id.positive?

          span.set_attribute('db.mongodb.cursor_id', result.cursor_id)
        end

        def maybe_trace_error(result, span)
          return if result.successful?

          span.set_attribute('db.response.status_code', result.error.code.to_s)
        end

        def query_summary(message)
          if (coll_name = collection_name(message))
            "#{command_name(message)} #{database(message)}.#{coll_name}"
          else
            "#{command_name(message)} #{database(message)}"
          end
        end

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

        def command_name(message)
          message.documents.first.keys.first.to_s
        end

        def database(message)
          message.documents.first['$db'].to_s
        end

        def query_text?
          @query_text_max_length.positive?
        end

        def cursor_id(message)
          return unless command_name(message) == 'getMore'

          message.documents.first['getMore'].value
        end

        def lsid(message)
          lsid_doc = message.documents.first['lsid']
          return unless lsid_doc

          lsid_doc['id']
        end

        def txn_number(message)
          txn_num = message.documents.first['txnNumber']
          return unless txn_num

          txn_num.value
        end

        EXCLUDED_KEYS = %w[lsid $db $clusterTime signature].freeze
        ELLIPSES = '...'

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
