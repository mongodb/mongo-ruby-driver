require 'spec_helper'

describe 'Server description' do
  describe '#op_time' do
    require_topology :replica_set
    min_server_fcv '3.4'

    let(:client) { ClientRegistry.instance.global_client('authorized') }
    let(:desc) { client.cluster.servers.first.description }

    it 'is set' do
      client.database.command(ismaster: 1)

      expect(desc.op_time).to be_a(BSON::Timestamp)
    end
  end

  describe '#last_write_date' do
    require_topology :replica_set
    min_server_fcv '3.4'

    let(:client) { ClientRegistry.instance.global_client('authorized') }
    let(:desc) { client.cluster.servers.first.description }

    it 'is set' do
      client.database.command(ismaster: 1)

      expect(desc.last_write_date).to be_a(Time)
    end
  end

  describe '#last_update_time' do
    before do
      ClientRegistry.instance.close_all_clients
    end

    let(:client) { ClientRegistry.instance.global_client('authorized') }
    let(:desc) { client.cluster.next_primary.description }

    it 'is set' do
      client.database.command(ismaster: 1)

      expect(desc.last_update_time).to be_a(Time)
      # checked in the last 3 seconds
      expect(Time.now - desc.last_update_time < 3).to be true
    end
  end
end
