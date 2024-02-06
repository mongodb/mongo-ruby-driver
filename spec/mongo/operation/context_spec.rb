# frozen_string_literals: true

require 'lite_spec_helper'

describe Mongo::Operation::Context do
  describe '#initialize' do
    context 'when timeout_ms is negative' do
      it 'raises an error' do
        expect do
          described_class.new(timeout_ms: -1)
        end.to raise_error ArgumentError, /must be a positive integer/
      end
    end
  end

  describe '#deadline' do
    before do
      expect(Mongo::Utils).to receive(:monotonic_time).and_return(100.0)
    end

    let(:timeout_ms) { 10_000 }

    let(:context) { described_class.new(timeout_ms: timeout_ms) }

    it 'calculates the deadline' do
      expect(context.deadline).to eq(110)
    end
  end

  describe '#remaining_timeout_ms' do
    before do
      expect(Mongo::Utils).to receive(:monotonic_time).ordered.and_return(100.0)
      expect(Mongo::Utils).to receive(:monotonic_time).ordered.and_return(105.0)
    end

    let(:timeout_ms) { 10_000 }

    let(:context) { described_class.new(timeout_ms: timeout_ms) }

    it 'calculates the remaining time' do
      expect(context.remaining_timeout_ms).to eq(5_000)
    end
  end
end
