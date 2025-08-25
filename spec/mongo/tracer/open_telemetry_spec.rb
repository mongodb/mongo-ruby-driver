# frozen_string_literal: true

require 'spec_helper'

describe Mongo::Tracer::OpenTelemetry do
  describe '#initialize' do
    it 'disables OpenTelemetry by default' do
      tracer = described_class.new
      expect(tracer.enabled?).to be false
    end

    it 'disables OpenTelemetry when the environment variable is not set' do
      allow(ENV).to receive(:[]).with('OTEL_RUBY_INSTRUMENTATION_MONGODB_ENABLED').and_return(nil)
      tracer = described_class.new
      expect(tracer.enabled?).to be false
    end

    %w[ true 1 yes  ].each do |value|
      it "enables OpenTelemetry when the environment variable is set to '#{value}'" do
        allow(ENV).to receive(:[]).with('OTEL_RUBY_INSTRUMENTATION_MONGODB_ENABLED').and_return(value)
        tracer = described_class.new
        expect(tracer.enabled?).to be true
      end
    end
  end
end
