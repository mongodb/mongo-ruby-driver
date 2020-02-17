require 'spec_helper'

# Create a client with all possible configurations (forcing/discovering each
# topology type) and ensure the resulting client is usable.
describe 'Client construction' do
  let(:base_options) do
    SpecConfig.instance.test_options.merge(
      server_selection_timeout: 5,
      database: SpecConfig.instance.test_db,
    ).merge(SpecConfig.instance.credentials_or_x509(
      user: SpecConfig.instance.test_user.name,
      password: SpecConfig.instance.test_user.password,
    ))
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

    it 'creates connection pool and keeps it populated' do
      client = ClientRegistry.instance.new_local_client([SpecConfig.instance.addresses.first],
        base_options.merge(min_pool_size: 1))
      # allow connection pool to populate
      sleep 0.1

      server = client.cluster.next_primary
      expect(server.pool.size).to eq(1)
      client['client_construction'].insert_one(test: 1)
      expect(server.pool.size).to eq(1)
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
      primary_address = ClusterConfig.instance.primary_address_str
      client = ClientRegistry.instance.new_local_client([primary_address],
        base_options.merge(connect: :direct))
      client['client_construction'].insert_one(test: 1)
      expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::Single)
      expect(client.options[:connect]).to eq :direct
    end

    context 'direct connection with mismached me' do
      let(:address) { ClusterConfig.instance.alternate_address.to_s }

      let(:client) do
        new_local_client([address], SpecConfig.instance.test_options)
      end

      let(:server) { client.cluster.next_primary }

      it 'sets server type to primary' do
        expect(server.description).to be_primary
      end
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
      primary_address = ClusterConfig.instance.primary_address_str
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

  context 'with auto encryption options'do
    require_libmongocrypt
    min_server_version '4.2'
    let(:options) { { auto_encryption_options: auto_encryption_options } }

    let(:auto_encryption_options) do
      {
        key_vault_client: key_vault_client,
        key_vault_namespace: 'database.collection',
        kms_providers: {
          local: { key: Base64.encode64('ruby' * 24) },
        }
      }
    end

    context 'with default key vault client' do
      let(:key_vault_client) { nil }

      let(:client) do
        ClientRegistry.instance.new_local_client([SpecConfig.instance.addresses.first], options)
      end

      after do
        client.teardown_encrypter
      end

      it 'creates a working key vault client' do
        key_vault_client = client.encryption_options['key_vault_client']
        expect(key_vault_client.encryption_options).to be_nil

        result = key_vault_client[:test].insert_one(test: 1)
        expect(result).to be_ok
      end

      it 'creates a key vault client with a different cluster from the existing client' do
        key_vault_client = client.encryption_options['key_vault_client']
        expect(key_vault_client.cluster).not_to eq(client.cluster)
      end
    end
  end

  context 'when seed addresses are repeated in host list' do
    require_topology :single

    let(:primary_address) do
      ClusterConfig.instance.primary_address_host
    end

    let(:client) do
      new_local_client([primary_address, primary_address], SpecConfig.instance.test_options)
    end

    it 'deduplicates the addresses' do
      expect(client.cluster.addresses).to eq([Mongo::Address.new(primary_address)])
    end
  end

  context 'when seed addresses are repeated in URI' do
    require_topology :single

    let(:primary_address) do
      ClusterConfig.instance.primary_address_host
    end

    let(:client) do
      new_local_client("mongodb://#{primary_address},#{primary_address}", SpecConfig.instance.test_options)
    end

    it 'deduplicates the addresses' do
      expect(client.cluster.addresses).to eq([Mongo::Address.new(primary_address)])
    end
  end
end
