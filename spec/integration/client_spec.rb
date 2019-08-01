require 'spec_helper'

describe 'Client' do
  context 'after client is disconnected' do
    let(:client) { authorized_client.with(server_selection_timeout: 1) }

    before do
      client.close
    end

    it 'is still usable for operations' do
      resp = client.database.command(ismaster: 1)
      expect(resp).to be_a(Mongo::Operation::Result)
    end

    it 'is still usable for operations that can use sessions' do
      client['collection'].insert_one(test: 1)
    end

    context 'after all servers are marked unknown' do
      before do
        client.cluster.servers.each do |server|
          server.unknown!
        end
      end

      context 'operation that never uses sessions' do
        it 'fails server selection' do
          expect do
            client.database.command(ismaster: 1)
          end.to raise_error(Mongo::Error::NoServerAvailable)
        end
      end

      context 'operation that can use sessions' do
        it 'fails server selection' do
          expect do
            client['collection'].insert_one(test: 1)
          end.to raise_error(Mongo::Error::NoServerAvailable)
        end
      end
    end
  end
end
