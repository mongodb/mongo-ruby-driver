# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Auth::Oidc::MachineWorkflow do
  let(:callback) do
    double('callback')
  end

  let(:properties) do
    { oidc_callback: callback }
  end

  describe '#start' do
    context 'when executing for the first time' do
      let(:workflow) do
        described_class.new(auth_mech_properties: properties)
      end

      let(:token) do
        'token'
      end

      before do
        expect(callback).to receive(:execute).with(
          timeout: 60000,
          version: 1,
          username: nil
        ).and_return({ access_token: token })
      end

      let(:result) do
        workflow.execute
      end

      it 'returns the token result' do
        expect(result).to eq({ access_token: token })
      end
    end

    context 'when executing multiple times in succession' do
      let!(:workflow) do
        described_class.new(auth_mech_properties: properties)
      end

      let(:token) do
        'token'
      end

      let!(:time) do
        Process.clock_gettime(Process::CLOCK_MONOTONIC, :millisecond)
      end

      before do
        expect(callback).to receive(:execute).exactly(10).times.and_return({ access_token: token })
      end

      it 'throttles the execution at 100ms' do
        10.times do
          workflow.execute
        end
        current_time = Process.clock_gettime(Process::CLOCK_MONOTONIC, :millisecond)
        # TODO: Best way to test throttling?
      end
    end
  end
end
