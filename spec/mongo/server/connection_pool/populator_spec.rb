# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Server::ConnectionPool::Populator do
  require_no_linting

  let(:options) { {} }

  let(:client) do
    authorized_client.with(options)
  end

  let(:server) do
    client.cluster.next_primary
  end

  let(:pool) do
    server.pool
  end

  let(:populator) do
    register_background_thread_object(
      described_class.new(pool, pool.options)
    )
  end

  before do
    # We create our own populator to test; disable pool's background populator
    # and clear the pool, so ours can run
    pool.disconnect!
    pool.stop_populator
  end

  describe '#log_warn' do
    it 'works' do
      expect do
        populator.log_warn('test warning')
      end.not_to raise_error
    end
  end


  describe '#run!' do
    context 'when the min_pool_size is zero' do
      let(:options) { {min_pool_size: 0} }

      it 'calls populate on pool once' do
        expect(pool).to receive(:populate).once.and_call_original
        populator.run!
        sleep 1
        expect(populator.running?).to be true
      end
    end

    context 'when the min_pool_size is greater than zero' do
      let(:options) { {min_pool_size: 2, max_pool_size: 3} }

      it 'calls populate on the pool multiple times' do
        expect(pool).to receive(:populate).at_least(:once).and_call_original
        populator.run!
        sleep 1
        expect(populator.running?).to be true
      end

      it 'populates the pool up to min_size' do
        pool.instance_variable_set(:@ready, true)
        populator.run!
        ::Utils.wait_for_condition(3) do
          pool.size >= 2
        end
        expect(pool.size).to eq 2
        expect(populator.running?).to be true
      end
    end

    context 'when populate raises a non socket related error' do
      it 'does not terminate the thread' do
        expect(pool).to receive(:populate).once.and_raise(Mongo::Auth::InvalidMechanism.new(""))
        populator.run!
        sleep 0.5
        expect(populator.running?).to be true
      end
    end

    context 'when populate raises a socket related error' do
      it 'does not terminate the thread' do
        expect(pool).to receive(:populate).once.and_raise(Mongo::Error::SocketError)
        populator.run!
        sleep 0.5
        expect(populator.running?).to be true
      end
    end

    context "when clearing the pool" do
      it "the populator is run one extra time" do
        expect(pool).to receive(:populate).twice
        populator.run!
        sleep 0.5
        pool.disconnect!
        sleep 0.5
        expect(populator.running?).to be true
      end
    end
  end

  describe '#stop' do
    it 'stops calling populate on pool and terminates the thread' do
      populator.run!

      # let populator do work and wait on semaphore
      sleep 0.5

      expect(pool).not_to receive(:populate)
      populator.stop!
      expect(populator.running?).to be false
    end
  end
end
