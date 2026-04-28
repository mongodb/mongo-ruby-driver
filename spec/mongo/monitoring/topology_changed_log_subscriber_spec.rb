# frozen_string_literal: true

require 'lite_spec_helper'

describe Mongo::Monitoring::TopologyChangedLogSubscriber do
  let(:subscriber) { described_class.new }
  let(:address) { Mongo::Address.new('127.0.0.1:27017') }

  let(:primary_config) do
    {
      'isWritablePrimary' => true,
      'secondary' => false,
      'setName' => 'rs0',
      'hosts' => [ '127.0.0.1:27017' ],
      'me' => '127.0.0.1:27017',
      'minWireVersion' => 0,
      'maxWireVersion' => 21,
      'ok' => 1,
    }
  end

  let(:primary_desc) { Mongo::Server::Description.new(address, primary_config) }

  before { Mongo::Logger.level = Logger::DEBUG }
  after { Mongo::Logger.level = Logger::INFO }

  # Build a topology instance without invoking its initializer (which requires
  # a real Cluster). The subscriber only reads `class`, `display_name`, and
  # `server_descriptions` on the topology, so this is sufficient.
  def fabricate_topology(klass, server_descriptions)
    topology = klass.allocate
    topology.instance_variable_set(:@server_descriptions, server_descriptions)
    topology
  end

  def make_event(prev_top, new_top)
    Mongo::Monitoring::Event::TopologyChanged.new(prev_top, new_top)
  end

  context 'when topologies have the same class and equal server_descriptions' do
    let(:server_descriptions) { { address.to_s => primary_desc } }
    let(:prev_top) do
      fabricate_topology(Mongo::Cluster::Topology::ReplicaSetWithPrimary, server_descriptions)
    end
    let(:new_top) do
      fabricate_topology(Mongo::Cluster::Topology::ReplicaSetWithPrimary, server_descriptions)
    end

    it 'does not log' do
      expect(subscriber).not_to receive(:log_debug)
      subscriber.succeeded(make_event(prev_top, new_top))
    end
  end

  context 'when topologies have the same class but different server_descriptions' do
    let(:other_address) { Mongo::Address.new('127.0.0.1:27018') }
    let(:other_desc) { Mongo::Server::Description.new(other_address, primary_config) }

    let(:prev_top) do
      fabricate_topology(
        Mongo::Cluster::Topology::ReplicaSetWithPrimary,
        { address.to_s => primary_desc }
      )
    end
    let(:new_top) do
      fabricate_topology(
        Mongo::Cluster::Topology::ReplicaSetWithPrimary,
        { address.to_s => primary_desc, other_address.to_s => other_desc }
      )
    end

    it 'logs the members-changed message' do
      expect(subscriber).to receive(:log_debug).with(
        a_string_matching(/There was a change in the members of the '.*' topology\./)
      )
      subscriber.succeeded(make_event(prev_top, new_top))
    end
  end

  context 'when topology class changes' do
    let(:server_descriptions) { { address.to_s => primary_desc } }
    let(:prev_top) do
      fabricate_topology(Mongo::Cluster::Topology::Unknown, server_descriptions)
    end
    let(:new_top) do
      fabricate_topology(Mongo::Cluster::Topology::ReplicaSetWithPrimary, server_descriptions)
    end

    it 'logs the type-change message' do
      expect(subscriber).to receive(:log_debug).with(
        a_string_matching(/Topology type '.*' changed to type '.*'\./)
      )
      subscriber.succeeded(make_event(prev_top, new_top))
    end
  end
end
