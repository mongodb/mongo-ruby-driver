# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Direct connection with RS name' do
  before(:all) do
    # preload
    ClusterConfig.instance.replica_set_name
  end

  clean_slate_for_all

  shared_examples_for 'passes RS name to topology' do
    it 'passes RS name to topology' do
      expect(client.cluster.topology.replica_set_name).to eq(replica_set_name)
    end
  end

  let(:client) do
    new_local_client(
      [SpecConfig.instance.addresses.first],
      SpecConfig.instance.test_options.merge(
        replica_set: replica_set_name, connect: :direct,
        server_selection_timeout: 3.32,
      ))
  end

  context 'in replica set' do
    require_topology :replica_set

    context 'with correct RS name' do
      let(:replica_set_name) { ClusterConfig.instance.replica_set_name }

      it_behaves_like 'passes RS name to topology'

      it 'creates a working client' do
        expect do
          res = client.database.command(ping: 1)
          p res
        end.not_to raise_error
      end
    end

    context 'with wrong RS name' do
      let(:replica_set_name) { 'wrong' }

      it_behaves_like 'passes RS name to topology'

      it 'creates a client which does not find a suitable server' do
        # TODO When RUBY-2197 is implemented, assert the error message also
        expect do
          client.database.command(ping: 1)
        end.to raise_error(Mongo::Error::NoServerAvailable)
      end
    end
  end

  context 'in standalone' do
    require_topology :single

    context 'with any RS name' do
      let(:replica_set_name) { 'any' }

      it_behaves_like 'passes RS name to topology'

      it 'creates a client which raises on every operation' do
        # TODO When RUBY-2197 is implemented, assert the error message also
        expect do
          client.database.command(ping: 1)
        end.to raise_error(Mongo::Error::NoServerAvailable)
      end
    end
  end
end
