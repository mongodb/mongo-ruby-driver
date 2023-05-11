# frozen_string_literal: true
# rubocop:todo all

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

    # This test requires a PSA deployment. The port number is fixed for our
    # Evergreen/Docker setups.
    context 'when directly connecting to arbiters' do
      let(:options) do
        SpecConfig.instance.test_options.tap do |opt|
          opt.delete(:connect)
          opt.delete(:replica_set)
          opt.update(direct_connection: true)
        end
      end

      let(:client) do
        new_local_client(['localhost:27019'], options)
      end

      let(:response) { client.command(ismaster: 1).documents.first }

      it 'connects' do
        response.fetch('arbiterOnly').should be true
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
        end.to raise_error(ArgumentError, /The access_key_id option must be a String with at least one character; it is currently an empty string/)
      end
    end

    context 'with default key vault client' do
      let(:key_vault_client) { nil }

      shared_examples 'creates a working key vault client' do
        it 'creates a working key vault client' do
          key_vault_client = client.encrypter.key_vault_client

          result = key_vault_client[:test].insert_one(test: 1)
          expect(result).to be_ok
        end
      end

      context 'when top-level max pool size is not 0' do
        include_examples 'creates a working key vault client'

        shared_examples 'limited connection pool' do
          it 'creates a key vault client with a different cluster than the existing client' do
            key_vault_client = client.encrypter.key_vault_client
            expect(key_vault_client.cluster).not_to eq(client.cluster)
          end

          # min pool size for the key vault client can be greater than 0
          # when the key vault client is the same as the top-level client.
          # This is OK because we aren't making any more connections for FLE,
          # the minimum was requested by application for its own needs.
          it 'uses min pool size 0 for key vault client' do
            key_vault_client = client.encrypter.key_vault_client
            key_vault_client.options[:min_pool_size].should be 0
          end
        end

        context 'when top-level max pool size is not specified' do
          before do
            client.options[:max_pool_size].should be nil
          end

          include_examples 'limited connection pool'

          it 'uses unspecified max pool size for key vault client' do
            key_vault_client = client.encrypter.key_vault_client
            key_vault_client.options[:max_pool_size].should be nil
          end
        end

        context 'when top-level max pool size is specified' do
          let(:options) do
            {
              auto_encryption_options: auto_encryption_options,
              max_pool_size: 42,
            }
          end

          include_examples 'limited connection pool'

          it 'uses the same max pool size for key vault client' do
            key_vault_client = client.encrypter.key_vault_client
            key_vault_client.options[:max_pool_size].should be 42
          end
        end
      end

      context 'when top-level max pool size is 0' do
        let(:options) do
          {
            auto_encryption_options: auto_encryption_options,
            max_pool_size: 0,
          }
        end

        before do
          client.options[:max_pool_size].should be 0
        end

        include_examples 'creates a working key vault client'

        it 'creates a key vault client with the same cluster as the existing client' do
          key_vault_client = client.encrypter.key_vault_client
          expect(key_vault_client.cluster).to eq(client.cluster)
        end
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

  context 'when deployment is not a sharded cluster' do
    require_topology :single, :replica_set

    let(:client) do
      ClientRegistry.instance.new_local_client(
        [SpecConfig.instance.addresses.first],
        SpecConfig.instance.test_options.merge(options),
      )
    end

    context 'when load-balanced topology is requested' do
      let(:options) do
        {connect: :load_balanced, replica_set: nil}
      end

      it 'creates the client successfully' do
        client.should be_a(Mongo::Client)
      end

      it 'fails all operations' do
        lambda do
          client.command(ping: true)
        end.should raise_error(Mongo::Error::BadLoadBalancerTarget)
      end
    end
  end

  context 'when in load-balanced mode' do
    require_topology :load_balanced

    let(:client) do
      ClientRegistry.instance.new_local_client(
        [SpecConfig.instance.addresses.first],
        SpecConfig.instance.test_options.merge(options),
      )
    end

    context 'when load-balanced topology is requested via the URI option' do
      let(:options) do
        {connect: nil, load_balanced: true}
      end

      it 'creates the client successfully' do
        client.should be_a(Mongo::Client)
      end

      it 'fails all operations' do
        lambda do
          client.command(ping: true)
        end.should raise_error(Mongo::Error::MissingServiceId)
      end
    end
  end
end
