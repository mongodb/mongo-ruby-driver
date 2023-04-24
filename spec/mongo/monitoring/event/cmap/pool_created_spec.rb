# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Monitoring::Event::Cmap::PoolCreated do

  describe '#summary' do

    let(:address) do
      Mongo::Address.new('127.0.0.1:27017')
    end

    let(:options) do
      {
         wait_queue_timeout: 3,
         min_pool_size: 5,
      }
    end

    declare_topology_double

    let(:pool) do
      server = make_server(:primary)
      Mongo::Server::ConnectionPool.new(server)
    end

    let(:event) do
      described_class.new(address, options, pool)
    end

    it 'renders correctly' do
      expect(event.summary).to eq("#<PoolCreated address=127.0.0.1:27017 options={:wait_queue_timeout=>3, :min_pool_size=>5} pool=0x#{pool.object_id}>")
    end
  end
end
