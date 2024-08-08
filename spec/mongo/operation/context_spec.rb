# frozen_string_literal: true

require 'lite_spec_helper'

describe Mongo::Operation::Context do
  describe '#initialize' do
    context 'when timeout_ms is negative' do
      it 'raises an error' do
        expect do
          described_class.new(operation_timeouts: { operation_timeout_ms: -1 })
        end.to raise_error ArgumentError, /must be a non-negative integer/
      end
    end
  end

  describe '#deadline' do
    let(:context) { described_class.new(operation_timeouts: { operation_timeout_ms: timeout_ms }) }

    context 'when timeout_ms is nil' do
      let(:timeout_ms) { nil }

      it 'returns nil' do
        expect(context.deadline).to be_nil
      end
    end

    context 'when timeout_ms is zero' do
      let(:timeout_ms) { 0 }

      it 'returns nil' do
        expect(context.deadline).to eq(0)
      end
    end

    context 'when timeout_ms is positive' do
      before do
        allow(Mongo::Utils).to receive(:monotonic_time).and_return(100.0)
      end

      let(:timeout_ms) { 10_000 }

      it 'calculates the deadline' do
        expect(context.deadline).to eq(110)
      end
    end
  end

  describe '#remaining_timeout_ms' do
    let(:context) { described_class.new(operation_timeouts: { operation_timeout_ms: timeout_ms }) }

    context 'when timeout_ms is nil' do
      let(:timeout_ms) { nil }

      it 'returns nil' do
        expect(context.remaining_timeout_ms).to be_nil
      end
    end

    context 'when timeout_ms is zero' do
      let(:timeout_ms) { 0 }

      it 'returns nil' do
        expect(context.remaining_timeout_ms).to be_nil
      end
    end

    context 'when timeout_ms is positive' do
      before do
        allow(Mongo::Utils).to receive(:monotonic_time).and_return(100.0, 105.0)
      end

      let(:timeout_ms) { 10_000 }

      it 'calculates the remaining time' do
        expect(context.remaining_timeout_ms).to eq(5_000)
      end
    end
  end
end
