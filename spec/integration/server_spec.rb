require 'spec_helper'

describe 'Server' do
  let(:client) { authorized_client }

  let(:server) { client.cluster.next_primary }

  let(:collection) { client['collection'] }
  let(:view) { Mongo::Collection::View.new(collection) }

  describe 'operations when client/cluster are disconnected' do
    shared_examples 'it performs read operations and receives the correct result type' do
      context 'normal server' do
        it 'can be used for reads' do
          result = view.send(:send_initial_query, server)
          expect(result).to be_a(result_class)
        end
      end

      context 'known server in disconnected cluster' do
        before do
          client.close
          expect(server).not_to be_unknown
        end

        it 'can be used for reads' do
          result = view.send(:send_initial_query, server)
          expect(result).to be_a(result_class)
        end
      end

      context 'unknown server in disconnected cluster' do
        before do
          client.close
          server.unknown!
          expect(server).to be_unknown
        end

        it 'can be used for reads' do
          result = view.send(:send_initial_query, server)
          expect(result).to be_a(result_class)
        end
      end
    end

    context 'for servers with FCV >= 3.4' do
      min_server_fcv '3.2'

      let(:result_class) { Mongo::Operation::Find::Result }

      it_behaves_like 'it performs read operations and receives the correct result type'
    end

    context 'for servers with FCV < 3.4' do
      # Find command was introduced in server version 3.2, so older versions should
      # receive legacy result types.
      max_server_fcv '3.0'

      let(:result_class) { Mongo::Operation::Find::Legacy::Result }

      it_behaves_like 'it performs read operations and receives the correct result type'
    end
  end
end
