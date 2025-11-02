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
      # OperationTracer is responsible for tracing MongoDB driver operations using OpenTelemetry.
      #
      # @api private
      class OperationTracer
        extend Forwardable

        def_delegators :@parent_tracer,
                       :cursor_context_map,
                       :parent_context_for,
                       :transaction_context_map,
                       :transaction_map_key

        # Initializes a new OperationTracer.
        #
        # @param otel_tracer [ OpenTelemetry::Trace::Tracer ] the OpenTelemetry tracer.
        # @param parent_tracer [ Mongo::Tracing::OpenTelemetry::Tracer ] the parent tracer
        #   for accessing shared context maps.
        #
        # @api private
        def initialize(otel_tracer, parent_tracer)
          @otel_tracer = otel_tracer
          @parent_tracer = parent_tracer
        end

        # Trace a MongoDB operation.
        #
        # Creates an OpenTelemetry span for the operation, capturing attributes such as
        # database name, collection name, operation name, and cursor ID. The span is finished
        # automatically when the operation completes or fails.
        #
        # @param operation [ Mongo::Operation ] the MongoDB operation to trace.
        # @param operation_context [ Mongo::Operation::Context ] the context of the operation.
        # @param op_name [ String | nil ] an optional name for the operation. If nil, the
        #   operation class name is used.
        #
        # @yield the block representing the operation to be traced.
        #
        # @return [ Object ] the result of the operation.
        #
        # @api private
        # rubocop:disable Lint/RescueException
        def trace_operation(operation, operation_context, op_name: nil)
          parent_context = parent_context_for(operation_context, operation.cursor_id)
          span = @otel_tracer.start_span(
            operation_span_name(operation, op_name),
            attributes: span_attributes(operation, op_name),
            with_parent: parent_context,
            kind: :client
          )
          ::OpenTelemetry::Trace.with_span(span) do |s, c|
            yield.tap do |result|
              process_cursor_context(result, operation.cursor_id, c, s)
            end
          end
        rescue Exception => e
          span&.record_exception(e)
          span&.status = ::OpenTelemetry::Trace::Status.error("Unhandled exception of type: #{e.class}")
          raise e
        ensure
          span&.finish
        end
        # rubocop:enable Lint/RescueException

        private

        # Returns the operation name from the provided name or operation class.
        #
        # @param operation [ Mongo::Operation ] the operation.
        # @param op_name [ String | nil ] optional operation name.
        #
        # @return [ String ] the operation name in lowercase.
        def operation_name(operation, op_name = nil)
          op_name || operation.class.name.split('::').last.downcase
        end

        # Builds span attributes for the operation.
        #
        # @param operation [ Mongo::Operation ] the operation.
        # @param op_name [ String | nil ] optional operation name.
        #
        # @return [ Hash ] OpenTelemetry span attributes following MongoDB semantic conventions.
        def span_attributes(operation, op_name)
          {
            'db.system' => 'mongodb',
            'db.namespace' => operation.db_name.to_s,
            'db.collection.name' => collection_name(operation),
            'db.operation.name' => operation_name(operation, op_name),
            'db.operation.summary' => operation_span_name(operation, op_name),
            'db.mongodb.cursor_id' => operation.cursor_id,
          }.compact
        end

        # Processes cursor context after operation execution.
        #
        # Updates the cursor context map based on the result. Removes closed cursors
        # and stores context for newly created cursors.
        #
        # @param result [ Object ] the operation result.
        # @param cursor_id [ Integer | nil ] the cursor ID before the operation.
        # @param context [ OpenTelemetry::Context ] the OpenTelemetry context.
        # @param span [ OpenTelemetry::Trace::Span ] the current span.
        def process_cursor_context(result, cursor_id, context, span)
          return unless result.is_a?(Cursor)

          if result.id.zero?
            # If the cursor is closed, remove it from the context map.
            cursor_context_map.delete(cursor_id)
          elsif result.id && cursor_id.nil?
            # New cursor created, store its context.
            cursor_context_map[result.id] = context
            span.set_attribute('db.mongodb.cursor_id', result.id)
          end
        end

        # Extracts the collection name from the operation.
        #
        # @param operation [ Mongo::Operation ] the operation.
        #
        # @return [ String | nil ] the collection name, or nil if not applicable.
        def collection_name(operation)
          if operation.respond_to?(:coll_name) && operation.coll_name
            operation.coll_name.to_s
          else
            case operation
            when Operation::Aggregate
              operation.spec[:selector][:aggregate]
            when Operation::Count
              operation.spec[:selector][:count]
            when Operation::Create
              operation.spec[:selector][:create]
            when Operation::Distinct
              operation.spec[:selector][:distinct]
            when Operation::Drop
              operation.spec[:selector][:drop]
            when Operation::WriteCommand
              operation.spec[:selector].values.first
            else
              return nil
            end.to_s
          end
        end

        # Generates the span name for the operation.
        #
        # @param operation [ Mongo::Operation ] the operation.
        # @param op_name [ String | nil ] optional operation name.
        #
        # @return [ String ] span name in format "operation_name db.collection" or "operation_name db".
        def operation_span_name(operation, op_name = nil)
          coll_name = collection_name(operation)
          if coll_name && !coll_name.empty?
            "#{operation_name(operation, op_name)} #{operation.db_name}.#{coll_name}"
          else
            "#{operation_name(operation, op_name)} #{operation.db_name}"
          end
        end
      end
    end
  end
end
