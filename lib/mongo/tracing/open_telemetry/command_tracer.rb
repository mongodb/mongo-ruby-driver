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
      class CommandTracer
        def initialize(otel_tracer, query_text_max_length: 0)
          @otel_tracer = otel_tracer
          @query_text_max_length = query_text_max_length
        end

        def trace_command(message, _operation_context, connection)
          @otel_tracer.in_span(
            command_span_name(message),
            attributes: span_attributes(message, connection),
            kind: :client
          ) do |span, _context|
            yield.tap do |result|
              if result.respond_to?(:cursor_id) && result.cursor_id.positive?
                span.set_attribute('db.mongodb.cursor_id', result.cursor_id)
              end
            end
          end
        end

        private

        def span_attributes(message, connection)
          {
            'db.system' => 'mongodb',
            'db.namespace' => message.documents.first['$db'],
            'db.collection.name' => collection_name(message),
            'db.operation.name' => message.documents.first.keys.first,
            'server.port' => connection.address.port,
            'server.address' => connection.address.host,
            'network.transport' => connection.transport.to_s,
            'db.mongodb.server_connection_id' => connection.server.description.server_connection_id,
            'db.mongodb.driver_connection_id' => connection.id,
            'db.query.text' => query_text(message)
          }.compact
        end

        def command_span_name(message)
          message.documents.first.keys.first
        end

        def collection_name(message)
          case message.documents.first.keys.first
          when 'getMore'
            message.documents.first['collection']
          else
            message.documents.first.values.first
          end
        end

        def query_text?
          @query_text_max_length.positive?
        end

        EXCLUDED_KEYS = %w[lsid $db $clusterTime signature].freeze
        ELLIPSES = '...'

        def query_text(message)
          return unless query_text?

          text = message
                 .documents
                 .first
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
