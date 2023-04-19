# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Transaction pinning' do
  let(:client) { authorized_client.with(max_pool_size: 4) }
  let(:collection_name) { 'tx_pinning' }
  let(:collection) { client[collection_name] }

  before do
    authorized_client[collection_name].insert_many([{test: 1}] * 200)
  end

  let(:server) { client.cluster.next_primary }

  clean_slate

  context 'non-lb' do
    require_topology :sharded
    min_server_fcv '4.2'

    # Start several transactions, then complete each of them.
    # Force each transaction to be on its own connection.

    before do
      client.reconnect if client.closed?
      4.times do |i|
        # Collections cannot be created inside transactions.
        client["tx_pin_t#{i}"].drop
        client["tx_pin_t#{i}"].create
      end
    end

    after do
      if pool = server.pool_internal
        pool.close
      end
    end

    it 'works' do
      sessions = []
      connections = []

      4.times do |i|
        session = client.start_session
        session.start_transaction
        client["tx_pin_t#{i}"].insert_one({test: 1}, session: session)
        session.pinned_server.should be_a(Mongo::Server)
        sessions << session
        connections << server.pool.check_out
      end

      server.pool.size.should == 4

      connections.each do |c|
        server.pool.check_in(c)
      end

      sessions.each_with_index do |session, i|
        client["tx_pin_t#{i}"].insert_one({test: 2}, session: session)
        session.commit_transaction
      end
    end
  end

  context 'lb' do
    require_topology :load_balanced
    min_server_fcv '4.2'

    # In load-balanced topology, we cannot create new connections to a
    # particular service.

    context 'when no connection is available' do
      require_no_linting

      before do
        client.reconnect if client.closed?
        client["tx_pin"].drop
        client["tx_pin"].create
      end

      it 'raises MissingConnection' do
        session = client.start_session
        session.start_transaction
        client["tx_pin"].insert_one({test: 1}, session: session)
        session.pinned_server.should be nil
        session.pinned_connection_global_id.should_not be nil

        server.pool.size.should == 1
        service_id = server.pool.instance_variable_get(:@available_connections).first.service_id
        server.pool.clear(service_id: service_id)
        server.pool.size.should == 0

        lambda do
          client["tx_pin"].insert_one({test: 2}, session: session)
        end.should raise_error(Mongo::Error::MissingConnection)
      end
    end

    context 'when connection is available' do

      before do
        client.reconnect if client.closed?
      end

      it 'uses the available connection' do
        sessions = []
        connections = []

        4.times do |i|
          session = client.start_session
          session.start_transaction
          client["tx_pin_t#{i}"].insert_one({test: 1}, session: session)
          session.pinned_server.should be nil
          session.pinned_connection_global_id.should_not be nil
          sessions << session
          connections << server.pool.check_out
        end

        server.pool.size.should == 4

        connections.each do |c|
          server.pool.check_in(c)
        end

        sessions.each_with_index do |session, i|
          client["tx_pin_t#{i}"].insert_one({test: 2}, session: session)
          session.commit_transaction
        end
      end
    end
  end
end
