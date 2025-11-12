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
  # Provides OpenTelemetry tracing capabilities for MongoDB operations.
  module Tracing
    # Creates a new OpenTelemetry tracer for instrumenting MongoDB operations.
    #
    # @param enabled [Boolean, nil] Whether tracing is enabled. Defautl to nil, which
    #   means it will check the environment variable OTEL_RUBY_INSTRUMENTATION_MONGODB_ENABLED.
    #   See +Mongo::Tracing::OpenTelemetry::Tracer+ for details.
    # @param query_text_max_length [Integer, nil] Maximum length for captured query text. Defaults to nil,
    #   which means it will check the environment variable OTEL_RUBY_INSTRUMENTATION_MONGODB_QUERY_TEXT_MAX_LENGTH.
    #   See +Mongo::Tracing::OpenTelemetry::Tracer+ for details.
    # @param otel_tracer [OpenTelemetry::Trace::Tracer, nil] Custom OpenTelemetry tracer instance.
    #
    # @return [Mongo::Tracing::OpenTelemetry::Tracer] Configured tracer instance.
    def create_tracer(enabled: nil, query_text_max_length: nil, otel_tracer: nil)
      OpenTelemetry::Tracer.new(
        enabled: enabled,
        query_text_max_length: query_text_max_length,
        otel_tracer: otel_tracer
      )
    end
    module_function :create_tracer
  end
end

require 'mongo/tracing/open_telemetry'
