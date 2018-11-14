require 'spec_helper'

describe 'Server description' do
  describe '#op_time' do
    require_topology :replica_set
    min_server_version '3.4'

    let(:client) { ClientRegistry.instance.global_client('authorized') }
    let(:desc) { client.cluster.servers.first.description }

    it 'is set' do
      client.database.command(ismaster: 1)

      expect(desc.op_time).to be_a(BSON::Timestamp)
    end
  end

  describe '#last_write_date' do
    require_topology :replica_set
    min_server_version '3.4'

    let(:client) { ClientRegistry.instance.global_client('authorized') }
    let(:desc) { client.cluster.servers.first.description }

    it 'is set' do
      client.database.command(ismaster: 1)

      expect(desc.last_write_date).to be_a(Time)
    end
  end
end
