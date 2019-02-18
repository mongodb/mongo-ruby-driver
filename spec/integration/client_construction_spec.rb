require 'spec_helper'

# Create a client with all possible configurations (forcing/discovering each
# topology type) and ensure the resulting client is usable.
describe 'Client construction' do
  let(:base_options) do
    SpecConfig.instance.test_options.merge(
      server_selection_timeout: 5,
      database: SpecConfig.instance.test_db,
      user: SpecConfig.instance.test_user.name,
      password: SpecConfig.instance.test_user.password)
  end

  context 'in single topology' do
    require_topology :single

    it 'discovers standalone' do
      options = base_options.dup
      options.delete(:connect)
      client = ClientRegistry.instance.new_local_client([SpecConfig.instance.addresses.first],
        options)
      client['client_construction'].insert_one(test: 1)
      expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::Single)
      expect(client.options[:connect]).to be nil
    end

    it 'connects directly' do
      client = ClientRegistry.instance.new_local_client([SpecConfig.instance.addresses.first],
        base_options.merge(connect: :direct))
      client['client_construction'].insert_one(test: 1)
      expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::Single)
      expect(client.options[:connect]).to eq :direct
    end
  end

  context 'in replica set topology' do
    require_topology :replica_set

    it 'discovers replica set' do
      options = base_options.dup
      options.delete(:connect)
      options.delete(:replica_set)
      client = ClientRegistry.instance.new_local_client([SpecConfig.instance.addresses.first],
        options)
      client['client_construction'].insert_one(test: 1)
      expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::ReplicaSetWithPrimary)
      expect(client.options[:connect]).to be nil
      expect(client.options[:replica_set]).to be nil
    end

    it 'forces replica set' do
      replica_set_name = ClusterConfig.instance.replica_set_name
      expect(replica_set_name).not_to be nil
      client = ClientRegistry.instance.new_local_client([SpecConfig.instance.addresses.first],
        base_options.merge(connect: :replica_set,
          replica_set: replica_set_name))
      client['client_construction'].insert_one(test: 1)
      expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::ReplicaSetWithPrimary)
      expect(client.options[:connect]).to be :replica_set
      expect(client.options[:replica_set]).to eq(replica_set_name)
    end

    it 'connects directly' do
      primary_address = ClusterConfig.instance.primary_address
      client = ClientRegistry.instance.new_local_client([primary_address],
        base_options.merge(connect: :direct))
      client['client_construction'].insert_one(test: 1)
      expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::Single)
      expect(client.options[:connect]).to eq :direct
    end
  end

  context 'in sharded topology' do
    require_topology :sharded

    it 'connects to sharded cluster' do
      options = base_options.dup
      options.delete(:connect)
      client = ClientRegistry.instance.new_local_client([SpecConfig.instance.addresses.first],
        base_options.merge(connect: :sharded))
      client['client_construction'].insert_one(test: 1)
      expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::Sharded)
      expect(client.options[:connect]).to be :sharded
    end

    it 'connects directly' do
      primary_address = ClusterConfig.instance.primary_address
      client = ClientRegistry.instance.new_local_client([SpecConfig.instance.addresses.first],
        base_options.merge(connect: :direct))
      client['client_construction'].insert_one(test: 1)
      expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::Single)
      expect(client.options[:connect]).to eq :direct
    end
  end

  context 'when time is frozen' do
    let(:now) { Time.now }
    before do
      allow(Time).to receive(:now).and_return(now)
    end

    it 'connects' do
      client = ClientRegistry.instance.new_local_client([SpecConfig.instance.addresses.first],
        SpecConfig.instance.test_options)
      expect(client.cluster.topology).not_to be_a(Mongo::Cluster::Topology::Unknown)
    end
  end
end
