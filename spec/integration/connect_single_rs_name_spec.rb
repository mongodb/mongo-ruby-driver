require 'spec_helper'

describe 'Direct connection with RS name' do
  before(:all) do
    ClientRegistry.instance.close_all_clients
  end

  shared_examples_for 'passes RS name to topology' do
    it 'passes RS name to topology' do
      expect(client.cluster.topology.replica_set_name).to eq(replica_set_name)
    end
  end

  let(:client) do
    new_local_client(
      [SpecConfig.instance.addresses.first],
      replica_set: replica_set_name,
      connect: :direct)
  end

  context 'in replica set' do
    require_topology :replica_set

    context 'with correct RS name' do
      let(:replica_set_name) { SpecConfig.instance.replica_set_name }

      it_behaves_like 'passes RS name to topology'

      it 'creates a working client' do
        expect do
          client.database.command(ismaster: 1)
        end.not_to raise_error
      end
    end

    context 'with wrong RS name' do
      let(:replica_set_name) { 'wrong' }

      it_behaves_like 'passes RS name to topology'

      it 'creates a client which raises on every operation' do
        expect do
          client.database.command(ismaster: 1)
        end.to raise_error(Mongo::Error::NoServerAvailable)
      end
    end
  end
end
