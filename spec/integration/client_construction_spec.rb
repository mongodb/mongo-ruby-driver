require 'spec_helper'

# Create a client with all possible configurations (forcing/discovering each
# topology type) and ensure the resulting client is usable.
describe 'Client construction' do
  let(:base_options) do
    SpecConfig.instance.test_options.merge(
      server_selection_timeout: 5,
      database: SpecConfig.instance.test_db,
    ).merge(SpecConfig.instance.credentials_or_external_user(
      user: SpecConfig.instance.test_user.name,
      password: SpecConfig.instance.test_user.password,
      auth_source: 'admin',
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
        base_options.merge(min_pool_size: 1, max_pool_size: 1))
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
    require_enterprise
    min_server_fcv '4.2'

    # Diagnostics of leaked background threads only, these tests do not
    # actually require a clean slate. https://jira.mongodb.org/browse/RUBY-2138
    clean_slate

    include_context 'define shared FLE helpers'
    include_context 'with local kms_providers'

    let(:options) { { auto_encryption_options: auto_encryption_options } }

    let(:auto_encryption_options) do
      {
        key_vault_client: key_vault_client,
        key_vault_namespace: key_vault_namespace,
        kms_providers: kms_providers,
        # Spawn mongocryptd on non-default port for sharded cluster tests
        extra_options: extra_options,
      }
    end

    let(:client) do
      ClientRegistry.instance.new_local_client([SpecConfig.instance.addresses.first], options)
    end

    context 'with AWS kms providers with empty string credentials' do
      let(:auto_encryption_options) do
        {
          key_vault_namespace: key_vault_namespace,
          kms_providers: {
            aws: {
              access_key_id: '',
              secret_access_key: '',
            }
          },
          # Spawn mongocryptd on non-default port for sharded cluster tests
          extra_options: extra_options,
        }
      end

      it 'raises an exception' do
        expect do
          client
        end.to raise_error(ArgumentError, /The aws access_key_id option must be a String with at least one character; it is currently an empty string/)
      end
    end

    context 'with default key vault client' do
      let(:key_vault_client) { nil }

      it 'creates a working key vault client' do
        key_vault_client = client.encrypter.key_vault_client

        result = key_vault_client[:test].insert_one(test: 1)
        expect(result).to be_ok
      end

      it 'creates a key vault client with the same cluster as the existing client' do
        key_vault_client = client.encrypter.key_vault_client
        expect(key_vault_client.cluster).to eq(client.cluster)
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
