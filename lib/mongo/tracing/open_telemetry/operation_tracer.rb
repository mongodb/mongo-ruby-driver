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
          @otel_tracer.in_span(
            operation_span_name(name, operation),
            attributes: span_attributes(name, operation),
            kind: :client
          ) do |span, _context|
            operation_context.tracer = @parent_tracer
            yield.tap do |result|
              if result.is_a?(Cursor) && result.id.positive?
                span.set_attribute('db.mongodb.cursor_id', result.id)
              end
            end
          end
        ensure
          operation_context.tracer = nil
        end

        private

        # Returns a hash of attributes for the OpenTelemetry span for the operation.
        #
        # @param name [String] The name of the operation.
        # @param operation [Operation] The operation being traced.
        # @return [Hash] A hash of attributes for the span.
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

        # Returns the name of the span for the operation.
        #
        # @param name [String] The name of the operation.
        # @param operation [Operation] The operation being traced.
        # # @return [String] The name of the span.
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
