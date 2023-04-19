# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Cursor pinning' do
  let(:client) do
    authorized_client.tap do |client|
      client.reconnect if client.closed?
    end
  end
  let(:collection_name) { 'cursor_pinning' }
  let(:collection) { client[collection_name] }

  before do
    authorized_client[collection_name].insert_many([{test: 1}] * 200)
  end

  let(:server) { client.cluster.next_primary }

  clean_slate

  context 'non-lb' do
    require_topology :single, :replica_set, :sharded
    require_no_multi_mongos

    # When not in load-balanced topology, iterating a cursor creates
    # new connections as needed.

    it 'creates new connections for iteration' do
      server.pool.size.should == 0

      # Use batch_size of 2 until RUBY-2727 is fixed.
      enum = collection.find({}, batch_size: 2).to_enum
      # Still zero because we haven't iterated
      server.pool.size.should == 0

      enum.next
      enum.next
      server.pool.size.should == 1

      # Grab the connection that was used
      server.with_connection do
        # This requires a new connection
        enum.next

        server.pool.size.should == 2
      end
    end
  end

  context 'lb' do
    require_topology :load_balanced

    # In load-balanced topology, we cannot create new connections to a
    # particular service.

    context 'when no connection is available' do

      it 'raises ConnectionCheckOutTimeout' do
        server.pool.size.should == 0

        enum = collection.find({}, batch_size: 1).to_enum
        # Still zero because we haven't iterated
        server.pool.size.should == 0

        enum.next
        server.pool.size.should == 1

        # Grab the connection that was used
        server.with_connection do
          # This requires a new connection, but we cannot make one.
          lambda do
            enum.next
          end.should raise_error(Mongo::Error::ConnectionCheckOutTimeout)

          server.pool.size.should == 1
        end
      end
    end

    context 'when connection is available' do
      require_multi_mongos

      let(:client) { authorized_client.with(max_pool_size: 4) }

      it 'uses the available connection' do
        server.pool.size.should == 0

        # Create 4 connections.

        enums = []
        connections = []
        connection_ids = []

        4.times do
          view = collection.find({}, batch_size: 1)
          enum = view.to_enum

          enum.next

          enums << enum
          connection_ids << view.cursor.initial_result.connection_global_id
          connections << server.pool.check_out
        end

        connection_ids.uniq.length.should be > 1

        server.pool.size.should == 4

        connections.each do |c|
          server.pool.check_in(c)
        end

        # At this point, in theory, all connections are equally likely to
        # be chosen, but we have cursors referencing more than one
        # distinct service.
        # Iterate each cursor to ensure they all continue to work.
        enums.each do |enum|
          enum.next
        end
      end
    end
  end
end
