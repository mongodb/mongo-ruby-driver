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
        def initialize(otel_tracer, parent_tracer)
          @otel_tracer = otel_tracer
          @parent_tracer = parent_tracer
          @operation_names = {}
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
        # rubocop:disable Lint/RescueException
        def trace_operation(operation, operation_context, op_name: nil, &block)
          span = create_operation_span(operation, operation_context, op_name)
          # An invalid context has no trace identity: it cannot be propagated,
          # continued, or correlated with anything downstream, so every
          # operation on it is waste. This is a state check on the span we
          # were handed, not detection of whether the SDK is available — a
          # custom API-only provider returning real spans sees the full path.
          # Must not key on recording?: an unsampled-but-valid context still
          # has to be made current for propagation. Skipping here also leaves
          # the cursor context map untouched, consistent with tracing being
          # effectively off.
          return yield unless span.context.valid?

          if span.recording? && !operation.cursor_id.nil?
            span.set_attribute('db.mongodb.cursor_id', operation.cursor_id)
          end
          execute_with_span(span, operation, &block)
        rescue Exception => e
          handle_span_exception(span, e)
          raise e
        ensure
          span&.finish
        end
        # rubocop:enable Lint/RescueException

        private

        # Creates an OpenTelemetry span for the operation. The operation name,
        # collection name and span name are each computed once and reused
        # between the span name and the attributes.
        #
        # @param operation [ Mongo::Operation ] the operation.
        # @param operation_context [ Mongo::Operation::Context ] the operation context.
        # @param op_name [ String | nil ] optional operation name.
        #
        # @return [ OpenTelemetry::Trace::Span ] the created span.
        def create_operation_span(operation, operation_context, op_name)
          parent_context = parent_context_for(operation_context, operation.cursor_id)
          name = operation_name(operation, op_name)
          coll_name = collection_name(operation)
          span_name = operation_span_name(name, operation.db_name, coll_name)
          @otel_tracer.start_span(
            span_name,
            attributes: span_attributes(operation, name, span_name, coll_name),
            with_parent: parent_context,
            kind: :client
          )
        end

        # Executes the operation block within the span context.
        #
        # @param span [ OpenTelemetry::Trace::Span ] the span.
        # @param operation [ Mongo::Operation ] the operation.
        #
        # @yield the block to execute.
        #
        # @return [ Object ] the result of the block.
        def execute_with_span(span, operation)
          ::OpenTelemetry::Trace.with_span(span) do |s, c|
            yield.tap do |result|
              process_cursor_context(result, operation.cursor_id, c, s)
            end
          end
        end

        # Handles exception for the span.
        #
        # @param span [ OpenTelemetry::Trace::Span ] the span.
        # @param exception [ Exception ] the exception.
        def handle_span_exception(span, exception)
          return unless span

          span.record_exception(exception)
          span.status = ::OpenTelemetry::Trace::Status.error(
            "Unhandled exception of type: #{exception.class}"
          )
        end

        # Returns the operation name from the provided name or operation class.
        # The class-derived name is memoized per class: it is invariant and
        # deriving it (split + downcase) allocates on every call. The number
        # of operation classes is small and fixed, so the cache is unbounded
        # by design; a benign race on first computation writes identical
        # strings.
        #
        # @param operation [ Mongo::Operation ] the operation.
        # @param op_name [ String | nil ] optional operation name.
        #
        # @return [ String ] the operation name in lowercase.
        def operation_name(operation, op_name = nil)
          return op_name if op_name

          klass = operation.class
          @operation_names[klass] ||= klass.name.split('::').last.downcase
        end

        # Builds the attributes passed at span creation: the cheap set. The
        # cursor id is deferred to trace_operation (behind recording?) — on a
        # non-recording span set_attribute discards, and the sampler has no
        # use for it.
        #
        # @param operation [ Mongo::Operation ] the operation.
        # @param name [ String ] the operation name.
        # @param span_name [ String ] the span name, reused as the summary.
        # @param coll_name [ String | nil ] the collection name, computed once.
        #
        # @return [ Hash ] OpenTelemetry span attributes following MongoDB semantic conventions.
        def span_attributes(operation, name, span_name, coll_name)
          attrs = {
            'db.system.name' => 'mongodb',
            'db.namespace' => operation.db_name.to_s,
            'db.operation.name' => name,
            'db.operation.summary' => span_name
          }
          attrs['db.collection.name'] = coll_name unless coll_name.nil?
          attrs
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
          return operation.coll_name.to_s if operation.respond_to?(:coll_name) && operation.coll_name

          extract_collection_from_spec(operation)
        end

        # Extracts collection name from operation spec based on operation type.
        #
        # @param operation [ Mongo::Operation ] the operation.
        #
        # @return [ String | nil ] the collection name, or nil if not found.
        def extract_collection_from_spec(operation)
          collection_key = collection_key_for_operation(operation)
          return nil unless collection_key

          value = if collection_key == :first_value
                    operation.spec[:selector].values.first
                  else
                    operation.spec[:selector][collection_key]
                  end
          value&.to_s
        end

        # Returns the collection key for a given operation type.
        #
        # @param operation [ Mongo::Operation ] the operation.
        #
        # @return [ Symbol | nil ] the collection key symbol or nil.
        def collection_key_for_operation(operation)
          case operation
          when Operation::Aggregate then :aggregate
          when Operation::Count then :count
          when Operation::Create then :create
          when Operation::Distinct then :distinct
          when Operation::Drop then :drop
          when Operation::WriteCommand then :first_value
          end
        end

        # Generates the span name for the operation.
        #
        # @param name [ String ] the operation name.
        # @param db_name [ String ] the database name.
        # @param coll_name [ String | nil ] the collection name, if any.
        #
        # @return [ String ] span name in format "operation_name db.collection" or "operation_name db".
        def operation_span_name(name, db_name, coll_name)
          if coll_name && !coll_name.empty?
            "#{name} #{db_name}.#{coll_name}"
          else
            "#{name} #{db_name}"
          end
        end
      end
    end
  end
end
