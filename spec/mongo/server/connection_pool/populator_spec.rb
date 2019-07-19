require 'spec_helper'

describe Mongo::Server::ConnectionPoolPopulator do
  let(:server) do
    authorized_client.cluster.next_primary
  end

  let(:pool) do
    server.pool
  end

  let(:populator) do
    described_class.new(pool, pool.options)
  end

  describe '#log_warn' do
    it 'works' do
      expect do
        populator.log_warn('test warning')
      end.not_to raise_error
    end
  end
end
