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

    # In load-balanced topology, a cursor retains the connection used to create
    # it until the cursor is closed.

    context 'when connection is available' do
      require_multi_mongos

      let(:client) { authorized_client.with(max_pool_size: 2) }

      it 'does not return connection to the pool if cursor not drained' do
        expect(server.pool).not_to receive(:check_in)
        enum = collection.find({}, batch_size: 1).to_enum
        # Get the first element only; cursor is not drained, so there should
        # be no check_in of the connection.
        enum.next
      end

      it 'returns connection to the pool when cursor is drained' do
        view = collection.find({}, batch_size: 1)
        enum = view.to_enum
        expect_any_instance_of(Mongo::Cursor).to receive(:check_in_connection)
        # Drain the cursor
        enum.each { |it| it.nil? }
      end
    end
  end
end
