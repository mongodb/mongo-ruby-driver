# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

describe 'Client' do
  context 'after client is disconnected' do
    let(:client) { authorized_client.with(server_selection_timeout: 1) }

    before do
      client.close
    end

    it 'is still usable for operations' do
      resp = client.database.command(ping: 1)
      expect(resp).to be_a(Mongo::Operation::Result)
    end

    it 'is still usable for operations that can use sessions' do
      client['collection'].insert_one(test: 1)
    end

    context 'after all servers are marked unknown' do
      require_topology :single, :replica_set, :sharded

      before do
        client.cluster.servers.each do |server|
          server.unknown!
        end
      end

      context 'operation that never uses sessions' do
        it 'fails server selection' do
          expect do
            client.database.command(ping: 1)
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
