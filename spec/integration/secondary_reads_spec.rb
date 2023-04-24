# rubocop:todo all
require 'spec_helper'

describe 'Secondary reads' do
  before do
    root_authorized_client.use('sr')['secondary_reads'].drop
    root_authorized_client.use('sr')['secondary_reads'].insert_one(test: 1)
  end

  shared_examples 'performs reads as per read preference' do

    %i(primary primary_preferred).each do |mode|

      context mode.inspect do

        let(:client) do
          root_authorized_client.with(read: {mode: mode}).use('sr')
        end

        it 'reads from primary' do
          start_stats = get_read_counters

          30.times do
            client['secondary_reads'].find.to_a
          end

          end_stats = get_read_counters

          end_stats[:secondary].should be_within(10).of(start_stats[:secondary])
          end_stats[:primary].should >= start_stats[:primary] + 30
        end
      end
    end

    %i(secondary secondary_preferred).each do |mode|

      context mode.inspect do
        let(:client) do
          root_authorized_client.with(read: {mode: mode}).use('sr')
        end

        it 'reads from secondaries' do
          start_stats = get_read_counters

          30.times do
            client['secondary_reads'].find.to_a
          end

          end_stats = get_read_counters

          end_stats[:primary].should be_within(10).of(start_stats[:primary])
          end_stats[:secondary].should >= start_stats[:secondary] + 30
        end
      end
    end
  end

  context 'replica set' do
    require_topology :replica_set

    include_examples 'performs reads as per read preference'
  end

  context 'sharded cluster' do
    require_topology :sharded

    include_examples 'performs reads as per read preference'
  end

  def get_read_counters
    client = ClientRegistry.instance.global_client('root_authorized')
    addresses = []
    if client.cluster.sharded?
      doc = client.use('admin').command(listShards: 1).documents.first
      doc['shards'].each do |shard|
        addresses += shard['host'].split('/').last.split(',')
      end
    else
      client.cluster.servers.each do |server|
        next unless server.primary? || server.secondary?
        addresses << server.address.seed
      end
    end
    stats = Hash.new(0)
    addresses.each do |address|
      ClientRegistry.instance.new_local_client(
        [address],
        SpecConfig.instance.all_test_options.merge(connect: :direct),
      ) do |c|
        server = c.cluster.servers.first
        next unless server.primary? || server.secondary?
        stat = c.command(serverStatus: 1).documents.first
        queries = stat['opcounters']['query']
        if server.primary?
          stats[:primary] += queries
        else
          stats[:secondary] += queries
        end
      end
    end
    stats
  end
end
