# frozen_string_literal: true

require 'spec_helper'

# In load-balanced topology the driver checks a connection out of the pool
# before executing the initial command of a cursor-returning operation, so
# that the cursor can retain it. If the initial command fails, no cursor
# exists to drain and check the connection back in; the connection must be
# checked in before the error propagates, otherwise the pool permanently
# loses a slot per failure and the process eventually cannot check out any
# connections at all.
describe 'Load-balanced initial command failure' do
  require_topology :load_balanced
  require_no_multi_mongos

  let(:client) do
    authorized_client.tap do |client|
      client.reconnect if client.closed?
    end
  end
  let(:collection_name) { 'lb_initial_command_leak' }
  let(:collection) { client[collection_name] }
  let(:server) { client.cluster.next_primary }
  let(:pool) { server.pool }

  before do
    authorized_client[collection_name].insert_many([ { test: 1 } ] * 10)
  end

  after do
    client.use(:admin).command(
      configureFailPoint: 'failCommand',
      mode: 'off'
    )
  end

  def checked_out_count
    # Sample the checked-out set in a single read; computing
    # pool.size - pool.available_count takes the pool lock twice and can
    # race with background actors (e.g. the cursor reaper) between reads.
    pool.instance_variable_get(:@checked_out_connections).length
  end

  shared_examples 'returns the connection to the pool on failure' do |command_name|
    before do
      client.use(:admin).command(
        configureFailPoint: 'failCommand',
        mode: { times: 1 },
        data: { failCommands: [ command_name ], errorCode: 100 }
      )
    end

    it 'does not leak the connection' do
      baseline = checked_out_count
      expect do
        operation.call
      end.to raise_error(Mongo::Error::OperationFailure)
      expect(checked_out_count).to eq(baseline)
    end

    it 'can run the operation again after the failure' do
      begin
        operation.call
      rescue Mongo::Error::OperationFailure
        nil
      end
      expect do
        operation.call
      end.not_to raise_error
    end
  end

  context 'find' do
    let(:operation) { -> { collection.find(test: 1).to_a } }

    include_examples 'returns the connection to the pool on failure', 'find'
  end

  context 'aggregate' do
    let(:operation) { -> { collection.aggregate([ { '$match' => { test: 1 } } ]).to_a } }

    include_examples 'returns the connection to the pool on failure', 'aggregate'
  end

  context 'listCollections' do
    let(:operation) { -> { client.database.list_collections } }

    include_examples 'returns the connection to the pool on failure', 'listCollections'
  end

  context 'listIndexes' do
    let(:operation) { -> { collection.indexes.to_a } }

    include_examples 'returns the connection to the pool on failure', 'listIndexes'
  end

  context 'mapReduce' do
    let(:operation) do
      lambda do
        collection.find.map_reduce(
          'function() { emit(this.test, 1) }',
          'function(key, values) { return 1 }'
        ).to_a
      end
    end

    include_examples 'returns the connection to the pool on failure', 'mapReduce'
  end

  context 'watch (change stream)' do
    let(:operation) { -> { collection.watch } }

    include_examples 'returns the connection to the pool on failure', 'aggregate'
  end

  context 'when cursor construction fails after a successful command' do
    before do
      allow_any_instance_of(Mongo::Cursor).to receive(:set_cursor_id)
        .and_raise(Mongo::Error::InternalDriverError, 'simulated cursor construction failure')
    end

    it 'does not leak the connection' do
      baseline = checked_out_count
      expect do
        collection.find(test: 1).to_a
      end.to raise_error(Mongo::Error::InternalDriverError, /simulated cursor construction failure/)
      expect(checked_out_count).to eq(baseline)
    end
  end

  context 'in a transaction' do
    let(:session) { client.start_session }

    context 'when the initial command fails with a transient transaction error' do
      before do
        client.use(:admin).command(
          configureFailPoint: 'failCommand',
          mode: { times: 1 },
          data: {
            failCommands: [ 'find' ],
            errorCode: 112,
            errorLabels: [ 'TransientTransactionError' ]
          }
        )
      end

      # Session#unpin checks the pinned connection back in when it handles
      # a transient transaction error; the initial command cleanup must not
      # check the same connection in a second time (that would raise
      # ArgumentError from the pool and mask the retryable error).
      it 'retries the transaction and does not leak the connection' do
        baseline = checked_out_count
        docs = nil
        expect do
          session.with_transaction do
            collection.insert_one({ test: 2 }, session: session)
            docs = collection.find({ test: 1 }, session: session).to_a
          end
        end.not_to raise_error
        expect(docs).not_to be_empty
        # After a commit the connection stays pinned to the session until
        # the session ends or starts another transaction.
        session.end_session
        expect(checked_out_count).to eq(baseline)
      end
    end

    context 'when running aggregate on the pinned connection' do
      it 'reuses the transaction pinned connection' do
        docs = nil
        expect do
          session.with_transaction do
            collection.insert_one({ test: 2 }, session: session)
            docs = collection.aggregate([ { '$match' => { test: 1 } } ], session: session).to_a
          end
        end.not_to raise_error
        expect(docs).not_to be_empty
      end
    end
  end
end
