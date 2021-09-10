# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

describe Mongo::Monitoring::Event::Cmap::ConnectionCheckedOut do

  describe '#summary' do

    let(:address) do
      Mongo::Address.new('127.0.0.1:27017')
    end

    let(:id) do
      1
    end

    declare_topology_double

    let(:pool) do
      server = make_server(:primary)
      Mongo::Server::ConnectionPool.new(server)
    end

    let(:event) do
      described_class.new(address, id, pool)
    end

    it 'renders correctly' do
      expect(event.summary).to eq("#<ConnectionCheckedOut address=127.0.0.1:27017 connection_id=1 pool=0x#{pool.object_id}>")
    end
  end
end