require 'spec_helper'

describe 'SRV Monitoring' do
  context 'with SRV lookups mocked at Resolver' do
    let(:srv_result) do
      double('srv result').tap do |result|
        allow(result).to receive(:empty?).and_return(false)
        allow(result).to receive(:address_strs).and_return(
          [ClusterConfig.instance.primary_address_str])
      end
    end

    let(:client) do
      allow_any_instance_of(Mongo::Srv::Resolver).to receive(:get_records).and_return(srv_result)
      allow_any_instance_of(Mongo::Srv::Resolver).to receive(:get_txt_options_string)

      new_local_client_nmio('mongodb+srv://foo.a.b', server_selection_timeout: 3.15)
    end

    context 'standalone/replica set' do
      require_topology :single, :replica_set

      it 'does not create SRV monitor' do
        expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::Unknown)

        client.cluster.run_sdam_flow(
          Mongo::Server::Description.new(ClusterConfig.instance.primary_address_str),
          ClusterConfig.instance.primary_description,
        )

        expect(client.cluster.topology).not_to be_a(Mongo::Cluster::Topology::Unknown)

        expect(client.cluster.instance_variable_get('@srv_monitor')).to be nil
      end
    end

    context 'sharded cluster' do
      require_topology :sharded

      it 'creates SRV monitor' do
        expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::Unknown)

        # Since we force the cluster to run sdam flow which creates a monitor,
        # we need to manually adjust its state.
        client.cluster.instance_variable_set('@connecting', true)

        client.cluster.run_sdam_flow(
          Mongo::Server::Description.new(ClusterConfig.instance.primary_address_str),
          ClusterConfig.instance.primary_description,
        )

        expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::Sharded)

        expect(client.cluster.instance_variable_get('@srv_monitor')).to be_a(Mongo::Cluster::SrvMonitor)

        # Close the client in the test rather than allowing our post-test cleanup
        # to take care of it, since the client references test doubles.
        client.close
      end
    end
  end

  # These tests require a sharded cluster to be launched on localhost:27017
  # and localhost:27018, plus internet connectivity for SRV record lookups.
  context 'end to end' do
    require_default_port_deployment

    # JRuby apparently does not implement non-blocking UDP I/O which is used
    # by RubyDNS:
    # NotImplementedError: recvmsg_nonblock is not implemented
    fails_on_jruby

    before(:all) do
      require 'support/dns'
    end

    let(:uri) do
      "mongodb+srv://test-fake.test.build.10gen.cc/?tls=#{SpecConfig.instance.ssl?}&tlsInsecure=true"
    end

    let(:logger) do
      Logger.new(STDERR, level: Logger::DEBUG)
    end

    let(:client) do
      new_local_client(uri,
        server_selection_timeout: 3.16,
        timeout: 8.11,
        connect_timeout: 8.12,
        resolv_options: {
          nameserver: 'localhost',
          nameserver_port: [['localhost', 5300], ['127.0.0.1', 5300]],
        },
        logger: logger,
      )
    end

    before do
      # Expedite the polling process
      allow_any_instance_of(Mongo::Cluster::SrvMonitor).to receive(:scan_interval).and_return(1)
    end

    context 'sharded cluster' do
      require_topology :sharded
      require_multi_shard

      it 'updates topology via SRV records' do

        rules = [
          ['_mongodb._tcp.test-fake.test.build.10gen.cc', :srv,
            [0, 0, 27017, 'localhost.test.build.10gen.cc'],
          ],
        ]

        mock_dns(rules) do
          client.cluster.next_primary
          expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::Sharded)

          address_strs = client.cluster.servers.map(&:address).map(&:seed).sort
          expect(address_strs).to eq(%w(
            localhost.test.build.10gen.cc:27017
          ))
        end

        # In Evergreen there are replica set nodes on the next port number
        # after mongos nodes, therefore the addresses in DNS need to accurately
        # reflect how many mongos we have.

        rules = [
          ['_mongodb._tcp.test-fake.test.build.10gen.cc', :srv,
            [0, 0, 27018, 'localhost.test.build.10gen.cc'],
            [0, 0, 27017, 'localhost.test.build.10gen.cc'],
          ],
        ]

        mock_dns(rules) do
          15.times do
            address_strs = client.cluster.servers_list.map(&:address).map(&:seed).sort
            if address_strs == %w(
                localhost.test.build.10gen.cc:27017
                localhost.test.build.10gen.cc:27018
              )
            then
              break
            end
            sleep 1
          end

          address_strs = client.cluster.servers_list.map(&:address).map(&:seed).sort
          expect(address_strs).to eq(%w(
            localhost.test.build.10gen.cc:27017
            localhost.test.build.10gen.cc:27018
          ))
        end

        # And because we have only two mongos in Evergreen, test removal
        # separately here.

        rules = [
          ['_mongodb._tcp.test-fake.test.build.10gen.cc', :srv,
            [0, 0, 27018, 'localhost.test.build.10gen.cc'],
          ],
        ]

        mock_dns(rules) do
          15.times do
            address_strs = client.cluster.servers_list.map(&:address).map(&:seed).sort
            if address_strs == %w(
                localhost.test.build.10gen.cc:27018
              )
            then
              break
            end
            sleep 1
          end

          address_strs = client.cluster.servers_list.map(&:address).map(&:seed).sort
          expect(address_strs).to eq(%w(
            localhost.test.build.10gen.cc:27018
          ))

          expect(client.cluster.srv_monitor).to be_running
        end
      end
    end

    context 'unknown topology' do

      it 'updates topology via SRV records' do

        rules = [
          ['_mongodb._tcp.test-fake.test.build.10gen.cc', :srv,
            [0, 0, 27999, 'localhost.test.build.10gen.cc'],
          ],
        ]

        mock_dns(rules) do
          expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::Unknown)

          address_strs = client.cluster.servers_list.map(&:address).map(&:seed).sort
          expect(address_strs).to eq(%w(
            localhost.test.build.10gen.cc:27999
          ))
        end

        rules = [
          ['_mongodb._tcp.test-fake.test.build.10gen.cc', :srv,
            [0, 0, 27998, 'localhost.test.build.10gen.cc'],
            [0, 0, 27999, 'localhost.test.build.10gen.cc'],
          ],
        ]

        mock_dns(rules) do
          15.times do
            address_strs = client.cluster.servers_list.map(&:address).map(&:seed).sort
            if address_strs == %w(
                localhost.test.build.10gen.cc:27998
                localhost.test.build.10gen.cc:27999
              )
            then
              break
            end
            sleep 1
          end

          address_strs = client.cluster.servers_list.map(&:address).map(&:seed).sort
          expect(address_strs).to eq(%w(
            localhost.test.build.10gen.cc:27998
            localhost.test.build.10gen.cc:27999
          ))
        end

        rules = [
          ['_mongodb._tcp.test-fake.test.build.10gen.cc', :srv,
            [0, 0, 27997, 'localhost.test.build.10gen.cc'],
          ],
        ]

        mock_dns(rules) do
          15.times do
            address_strs = client.cluster.servers_list.map(&:address).map(&:seed).sort
            if address_strs == %w(
                localhost.test.build.10gen.cc:27997
              )
            then
              break
            end
            sleep 1
          end

          address_strs = client.cluster.servers_list.map(&:address).map(&:seed).sort
          expect(address_strs).to eq(%w(
            localhost.test.build.10gen.cc:27997
          ))

          expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::Unknown)

          expect(client.cluster.srv_monitor).to be_running
        end
      end
    end

    context 'unknown to sharded' do
      require_topology :sharded

      it 'updates topology via SRV records' do

        rules = [
          ['_mongodb._tcp.test-fake.test.build.10gen.cc', :srv,
            [0, 0, 27999, 'localhost.test.build.10gen.cc'],
          ],
        ]

        mock_dns(rules) do
          expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::Unknown)

          address_strs = client.cluster.servers_list.map(&:address).map(&:seed).sort
          expect(address_strs).to eq(%w(
            localhost.test.build.10gen.cc:27999
          ))
        end

        rules = [
          ['_mongodb._tcp.test-fake.test.build.10gen.cc', :srv,
            [0, 0, 27017, 'localhost.test.build.10gen.cc'],
          ],
        ]

        mock_dns(rules) do
          15.times do
            address_strs = client.cluster.servers.map(&:address).map(&:seed).sort
            if address_strs == %w(
                localhost.test.build.10gen.cc:27017
              )
            then
              break
            end
            sleep 1
          end

          address_strs = client.cluster.servers.map(&:address).map(&:seed).sort
          expect(address_strs).to eq(%w(
            localhost.test.build.10gen.cc:27017
          ))
          expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::Sharded)

          expect(client.cluster.srv_monitor).to be_running
        end
      end
    end

    context 'unknown to replica set' do
      require_topology :replica_set

      it 'updates topology via SRV records then stops SRV monitor' do

        rules = [
          ['_mongodb._tcp.test-fake.test.build.10gen.cc', :srv,
            [0, 0, 27999, 'localhost.test.build.10gen.cc'],
          ],
        ]

        mock_dns(rules) do
          expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::Unknown)

          address_strs = client.cluster.servers_list.map(&:address).map(&:seed).sort
          expect(address_strs).to eq(%w(
            localhost.test.build.10gen.cc:27999
          ))
        end

        rules = [
          ['_mongodb._tcp.test-fake.test.build.10gen.cc', :srv,
            [0, 0, 27017, 'localhost.test.build.10gen.cc'],
          ],
        ]

        mock_dns(rules) do
          15.times do
            address_strs = client.cluster.servers.map(&:address).map(&:seed).sort
            if address_strs == %w(
                localhost.test.build.10gen.cc:27017
              )
            then
              break
            end
            sleep 1
          end

          address_strs = client.cluster.servers.map(&:address).map(&:seed).sort
          # The actual address will be localhost:27017 or 127.0.0.1:27017,
          # depending on how the replica set is configured.
          expect(address_strs.any? { |str| str =~ /27017/ }).to be true
          # Covers both NoPrimary and WithPrimary replica sets
          expect(client.cluster.topology).to be_a(Mongo::Cluster::Topology::ReplicaSetNoPrimary)

          expect(client.cluster.srv_monitor).not_to be_running
        end
      end
    end
  end
end
