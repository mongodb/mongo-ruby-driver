# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path('../../../lib', __dir__)

namespace :otel_attributes do
  desc 'Measures the SDK cost of each OpenTelemetry span attribute shape'
  task :span do
    require_relative '../span_cost'

    puts Mongo::OtelAttributes::SpanCost.run!
  end

  desc 'Measures the driver cost of building each command span attribute'
  task :construction do
    require_relative '../construction_cost'

    puts Mongo::OtelAttributes::ConstructionCost.run!
  end

  desc 'Runs both attribute-cost harnesses'
  task all: %i[ span construction ]
end
