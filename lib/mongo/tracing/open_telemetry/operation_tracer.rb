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
      # OperationTracer is responsible for tracing MongoDB operations using OpenTelemetry.
      #
      # It provides methods to trace driven operations.
      # @api private
      class OperationTracer
        def initialize(otel_tracer, parent_tracer)
          @otel_tracer = otel_tracer
          @parent_tracer = parent_tracer
        end

        def trace_operation(name, operation, operation_context)
          parent_context = parent_context_for(operation_context, operation.cursor_id)
          operation_context.tracer = @parent_tracer
          span = @otel_tracer.start_span(
            operation_span_name(name, operation),
            attributes: span_attributes(name, operation),
            with_parent: parent_context,
            kind: :client
          )
          ::OpenTelemetry::Trace.with_span(span) do |_s, c|
            yield.tap do |result|
              process_cursor_context(result, operation.cursor_id, c)
            end
          end
        rescue Exception => e
          span&.record_exception(e)
          span&.status = ::OpenTelemetry::Trace::Status.error("Unhandled exception of type: #{e.class}")
          raise e
        ensure
          span&.finish
          operation_context.tracer = nil
        end

        private

        def span_attributes(name, operation)
          {
            'db.system' => 'mongodb',
            'db.namespace' => operation.db_name.to_s,
            'db.collection.name' => operation.coll_name.to_s,
            'db.operation.name' => name,
            'db.operation.summary' => operation_span_name(name, operation),
            'db.cursor.id' => operation.cursor_id,
          }.compact
        end

        def parent_context_for(operation_context, cursor_id)
          if (key = transaction_map_key(operation_context.session))
            transaction_context_map[key]
          elsif cursor_id
            cursor_context_map[cursor_id]
          end
        end

        # This map is used to store OpenTelemetry context for cursor_id.
        # This allows to group all operations related to a cursor under the same context.
        #
        # # @return [Hash] a map of cursor_id to OpenTelemetry context.
        def cursor_context_map
          @cursor_context_map ||= {}
        end

        def process_cursor_context(result, cursor_id, context)
          return unless result.is_a?(Cursor)

          if result.id.zero?
            # If the cursor is closed, remove it from the context map.
            cursor_context_map.delete(cursor_id)
          elsif result.id && cursor_id.nil?
            # New cursor created, store its context.
            cursor_context_map[result.id] = context
          end
        end

        # This map is used to store OpenTelemetry context for transaction.
        #   This allows to group all operations related to a transaction under the same context.
        #
        #   @return [Hash] a map of transaction_id to OpenTelemetry context.
        def transaction_context_map
          @transaction_context_map ||= {}
        end

        # @param session [Mongo::Session] the session for which to get the transaction map key.
        def transaction_map_key(session)
          return if session.nil? || session.implicit? || !session.in_transaction?

          "#{session.id}-#{session.txn_num}"
        end

        def operation_span_name(name, operation)
          if operation.coll_name
            "#{name} #{operation.db_name}.#{operation.coll_name}"
          else
            "#{operation.db_name}.#{name}"
          end
        end
      end
    end
  end
end
