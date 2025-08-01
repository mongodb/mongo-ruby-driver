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
      class Tracer
        # @return [ OpenTelemetry::Trace::Tracer ] the OpenTelemetry tracer implementation
        #   used to create spans for MongoDB operations and commands.
        #
        # @api private
        attr_reader :otel_tracer

        # Initializes a new OpenTelemetry tracer.
        #
        # @param enabled [ Boolean | nil ] whether OpenTelemetry is enabled or not.
        #   If nil, it will check the environment variable
        #   OTEL_RUBY_INSTRUMENTATION_MONGODB_ENABLED.
        # @param otel_tracer [ OpenTelemetry::Trace::Tracer | nil ] the OpenTelemetry tracer
        #   implementation to use. If nil, it will use the default tracer from
        #   OpenTelemetry's tracer provider.
        def initialize(enabled: nil, query_text_max_length: nil, otel_tracer: nil)
          @enabled = if enabled.nil?
                       %w[true 1 yes].include?(ENV['OTEL_RUBY_INSTRUMENTATION_MONGODB_ENABLED']&.downcase)
                     else
                       enabled
                     end
          @query_text_max_length = if query_text_max_length.nil?
                                     ENV['OTEL_RUBY_INSTRUMENTATION_MONGODB_QUERY_TEXT_MAX_LENGTH'].to_i
                                   else
                                     query_text_max_length
                                   end
          @otel_tracer = otel_tracer || initialize_tracer
          @operation_tracer = OperationTracer.new(@otel_tracer, self)
          @command_tracer = CommandTracer.new(@otel_tracer, query_text_max_length: @query_text_max_length)
        end

        # Whether OpenTelemetry is enabled or not.
        #
        # # @return [Boolean] true if OpenTelemetry is enabled, false otherwise.
        def enabled?
          @enabled
        end

        def trace_operation(name, operation, operation_context, &block)
          return yield unless enabled?

          operation_context.tracer = self
          @operation_tracer.trace_operation(name, operation, operation_context, &block)
        end

        def trace_command(message, operation_context, connection, &block)
          return yield unless enabled?

          @command_tracer.trace_command(message, operation_context, connection, &block)
        end

        private

        def initialize_tracer
          if enabled?
            # Obtain the proper tracer from OpenTelemetry's tracer provider.
            ::OpenTelemetry.tracer_provider.tracer(
              'mongo-ruby-driver',
              Mongo::VERSION
            )
          else
            # No-op tracer when OpenTelemetry is not enabled.
            ::OpenTelemetry::Trace::Tracer.new
          end
        end
      end
    end
  end
end
