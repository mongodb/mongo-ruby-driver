# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Server' do
  let(:client) { authorized_client }

  let(:server) { client.cluster.next_primary }

  let(:collection) { client['collection'] }
  let(:view) { Mongo::Collection::View.new(collection) }

  describe 'operations when client/cluster are disconnected' do
    context 'it performs read operations and receives the correct result type' do
      context 'normal server' do
        it 'can be used for reads' do
          result = view.send(:send_initial_query, server)
          expect(result).to be_a(Mongo::Operation::Find::Result)
        end
      end

      context 'known server in disconnected cluster' do
        require_topology :single, :replica_set, :sharded
        require_no_linting

        before do
          server.disconnect!
          expect(server).not_to be_unknown
        end

        after do
          server.close
        end

        it 'can be used for reads' do
          # See also RUBY-3102.
          result = view.send(:send_initial_query, server)
          expect(result).to be_a(Mongo::Operation::Find::Result)
        end
      end

      context 'unknown server in disconnected cluster' do
        require_topology :single, :replica_set, :sharded
        require_no_linting

        before do
          client.close
          server.unknown!
          expect(server).to be_unknown
        end

        after do
          server.close
        end

        it 'is unusable' do
          # See also RUBY-3102.
          lambda do
            view.send(:send_initial_query, server)
          end.should raise_error(Mongo::Error::ServerNotUsable)
        end
      end
    end
  end
end
