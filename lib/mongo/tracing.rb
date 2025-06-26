# frozen_string_literal: true

module Mongo
  module Tracing
    def create_tracer(enabled: nil, query_text_max_length: nil, otel_tracer: nil)
      OpenTelemetry::Tracer.new(
        enabled: enabled,
        query_text_max_length: query_text_max_length,
        otel_tracer: otel_tracer,
      )
    end
    module_function :create_tracer
  end
end

require 'mongo/tracing/open_telemetry'
