require 'spec_helper'

describe 'Server' do
  let(:client) { authorized_client }

  let(:server) { client.cluster.next_primary }

  let(:collection) { client['collection'] }
  let(:view) { Mongo::Collection::View.new(collection) }

  describe 'operations when client/cluster are disconnected' do
    # Server versions lower than 3.4 use the legacy find result
    min_server_fcv '3.4'

    context 'normal server' do
      it 'can be used for reads' do
        result = view.send(:send_initial_query, server)
        expect(result).to be_a(Mongo::Operation::Find::Result)
      end
    end

    context 'known server in disconnected cluster' do
      before do
        client.close
        expect(server).not_to be_unknown
      end

      it 'can be used for reads' do
        result = view.send(:send_initial_query, server)
        expect(result).to be_a(Mongo::Operation::Find::Result)
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
        # Driver falls back to the oldest MongoDB protocol
        expect(result).to be_a(Mongo::Operation::Find::Legacy::Result)
      end
    end
  end
end
