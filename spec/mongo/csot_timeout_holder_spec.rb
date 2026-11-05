# frozen_string_literal: true

require 'lite_spec_helper'

describe Mongo::CsotTimeoutHolder do
  describe '#remaining_timeout_ms!' do
    let(:holder) do
      described_class.new(operation_timeouts: { operation_timeout_ms: 1 })
    end

    context 'when less than a millisecond of the budget remains' do
      # The holder is created with a 1 ms budget, then queried half a
      # millisecond later: the raw millisecond conversion truncates 0.5 to 0,
      # and 0 is the driver-wide sentinel for "unlimited" (Timeout.timeout(0)
      # is a no-op, and Socket::SSL#connect! treats a zero connect_timeout as
      # unbounded). A finite budget must never be reported as zero.
      before do
        allow(Mongo::Utils).to receive(:monotonic_time).and_return(100.0, 100.0005)
      end

      it 'reports at least one millisecond rather than zero' do
        expect(holder.remaining_timeout_ms!).to eq(1)
      end
    end
  end
end
