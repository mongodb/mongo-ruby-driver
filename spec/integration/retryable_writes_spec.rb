require 'spec_helper'

# The tests raise OperationFailure in socket reads. This is done for
# convenience to make the tests uniform between socket errors and operation
# failures; in reality a socket read will never raise OperationFailure as
# wire protocol parsing code raises this exception. For the purposes of
# testing retryable writes, it is acceptable to raise OperationFailure in
# socket reads because both exceptions end up getting handled in the same
# place by retryable writes code. The SDAM error handling test specifically
# checks server state (i.e. being marked unknown) and scanning behavior
# that is performed by the wire protocol code; this test omits scan assertions
# as otherwise it quickly becomes unwieldy.
describe 'Retryable writes integration tests' do
  include PrimarySocket

  before do
    authorized_collection.delete_many
  end

  let(:check_collection) do
    # Verify data in the collection using another client instance to avoid
    # having the verification read trigger cluster scans on the writing client
    subscribed_client[TEST_COLL]
  end

  let(:primary_connection) do
    client.database.command(ping: 1)
    expect(primary_server.pool.size).to eq(1)
    expect(primary_server.pool.available_count).to eq(1)
    primary_server.pool.instance_variable_get('@available_connections').last
  end

  shared_examples_for 'an operation that is retried' do

    context 'when the operation fails on the first attempt and succeeds on the second attempt' do

      before do
        wait_for_all_servers(client.cluster)

        allow(primary_socket).to receive(:write).and_raise(error)
      end

      context 'when the error is retryable' do

        before do
          expect(Mongo::Logger.logger).to receive(:warn).once.and_call_original
        end

        context 'when the error is a SocketError' do

          let(:error) do
            Mongo::Error::SocketError
          end

          it 'retries writes' do
            operation
            expect(expectation).to eq(successful_retry_value)
          end
        end

        context 'when the error is a SocketTimeoutError' do

          let(:error) do
            Mongo::Error::SocketTimeoutError
          end

          it 'retries writes' do
            operation
            expect(expectation).to eq(successful_retry_value)
          end
        end

        context 'when the error is a retryable OperationFailure' do

          let(:error) do
            Mongo::Error::OperationFailure.new('not master')
          end

          let(:reply) do
            make_not_master_reply
          end

          it 'retries writes' do
            operation
            expect(expectation).to eq(successful_retry_value)
          end
        end
      end

      context 'when the error is not retryable' do

        context 'when the error is a non-retryable OperationFailure' do

          let(:error) do
            Mongo::Error::OperationFailure.new('other error', code: 123)
          end

          it 'does not retry writes' do
            expect {
              operation
            }.to raise_error(Mongo::Error::OperationFailure, /other error/)
            expect(expectation).to eq(unsuccessful_retry_value)
          end
        end
      end
    end

    context 'when the operation fails on the first attempt and again on the second attempt' do

      before do
        allow(primary_socket).to receive(:write).and_raise(error)
      end

      context 'when the selected server does not support retryable writes' do

        before do
          legacy_primary = double('legacy primary', :'retry_writes?' => false)
          expect(collection).to receive(:select_server).and_return(primary_server, legacy_primary)
          expect(primary_socket).to receive(:write).and_raise(error)
        end

        context 'when the error is a SocketError' do

          let(:error) do
            Mongo::Error::SocketError
          end

          it 'does not retry writes and raises the original error' do
            expect {
              operation
            }.to raise_error(error)
            expect(expectation).to eq(unsuccessful_retry_value)
          end
        end

        context 'when the error is a SocketTimeoutError' do

          let(:error) do
            Mongo::Error::SocketTimeoutError
          end

          it 'does not retry writes and raises the original error' do
            expect {
              operation
            }.to raise_error(error)
            expect(expectation).to eq(unsuccessful_retry_value)
          end
        end

        context 'when the error is a retryable OperationFailure' do

          let(:error) do
            Mongo::Error::OperationFailure.new('not master')
          end

          it 'does not retry writes and raises the original error' do
            expect {
              operation
            }.to raise_error(error)
            expect(expectation).to eq(unsuccessful_retry_value)
          end
        end
      end

      [
        Mongo::Error::SocketError,
        Mongo::Error::SocketTimeoutError,
        Mongo::Error::OperationFailure.new('not master'),
        Mongo::Error::OperationFailure.new('node is recovering'),
      ].each do |retryable_error|

        context "when the first error is a #{retryable_error}" do

          let(:error) do
            retryable_error
          end

          before do
            wait_for_all_servers(client.cluster)
            bad_socket = primary_connection.address.socket(primary_connection.socket_timeout,
                                                           primary_connection.send(:ssl_options))
            good_socket = primary_connection.address.socket(primary_connection.socket_timeout,
                                                            primary_connection.send(:ssl_options))
            allow(bad_socket).to receive(:write).and_raise(second_error)
            allow(primary_connection.address).to receive(:socket).and_return(bad_socket, good_socket)
          end

          context 'when the second error is a SocketError' do

            let(:second_error) do
              Mongo::Error::SocketError
            end

            it 'does not retry writes and raises the second error' do
              expect {
                operation
              }.to raise_error(second_error)
              expect(expectation).to eq(unsuccessful_retry_value)
            end
          end

          context 'when the second error is a SocketTimeoutError' do

            let(:second_error) do
              Mongo::Error::SocketTimeoutError
            end

            it 'does not retry writes and raises the second error' do
              expect {
                operation
              }.to raise_error(second_error)
              expect(expectation).to eq(unsuccessful_retry_value)
            end
          end

          context 'when the second error is a retryable OperationFailure' do

            let(:second_error) do
              Mongo::Error::OperationFailure.new('not master')
            end

            it 'does not retry writes and raises the second error' do
              expect {
                operation
              }.to raise_error(second_error)
              expect(expectation).to eq(unsuccessful_retry_value)
            end
          end

          context 'when the second error is a non-retryable OperationFailure' do

            let(:second_error) do
              Mongo::Error::OperationFailure.new('other error')
            end

            it 'does not retry writes and raises the first error' do
              expect {
                operation
              }.to raise_error(error)
              expect(expectation).to eq(unsuccessful_retry_value)
            end
          end

          context 'when the second error is a another error' do

            let(:second_error) do
              StandardError
            end

            it 'does not retry writes and raises the first error' do
              expect {
                operation
              }.to raise_error(error)
              expect(expectation).to eq(unsuccessful_retry_value)
            end
          end
        end
      end
    end
  end

  shared_examples_for 'an operation that is not retried' do

    let!(:client) do
      authorized_client_without_retry_writes
    end

    before do
      expect(primary_socket).to receive(:write).exactly(:once).and_raise(Mongo::Error::SocketError)
    end

    it 'does not retry writes' do
      expect {
        operation
      }.to raise_error(Mongo::Error::SocketError)
      expect(expectation).to eq(unsuccessful_retry_value)
    end
  end

  shared_examples_for 'an operation that does not support retryable writes' do

    let!(:client) do
      authorized_client_with_retry_writes
    end

    let!(:collection) do
      client[TEST_COLL]
    end

    before do
      expect(primary_socket).to receive(:write).and_raise(Mongo::Error::SocketError)
    end

    it 'does not retry writes' do
      expect {
        operation
      }.to raise_error(Mongo::Error::SocketError)
      expect(expectation).to eq(unsuccessful_retry_value)
    end
  end

  shared_examples_for 'operation that is retried when server supports retryable writes' do
    context 'when the server supports retryable writes' do
      min_server_fcv '3.6'

      before do
        allow(primary_server).to receive(:retry_writes?).and_return(true)
      end

      context 'standalone' do
        require_topology :single

        it_behaves_like 'an operation that is not retried'
      end

      context 'replica set or sharded cluster' do
        require_topology :replica_set, :sharded

        it_behaves_like 'an operation that is retried'
      end
    end

    context 'when the server does not support retryable writes' do

      before do
        allow(primary_server).to receive(:retry_writes?).and_return(false)
      end

      it_behaves_like 'an operation that is not retried'
    end
  end

  shared_examples_for 'supported retryable writes' do

    context 'when the client has retry_writes set to true' do

      let!(:client) do
        authorized_client_with_retry_writes
      end

      context 'when the collection has write concern acknowledged' do

        let!(:collection) do
          client[TEST_COLL, write: {w: :majority}]
        end

        it_behaves_like 'operation that is retried when server supports retryable writes'
      end

      context 'when the collection has write concern unacknowledged' do

        let!(:collection) do
          client[TEST_COLL, write: { w: 0 }]
        end

        it_behaves_like 'an operation that is not retried'
      end
    end

    context 'when the client has retry_writes set to false' do

      let!(:client) do
        authorized_client_without_retry_writes
      end

      context 'when the collection has write concern acknowledged' do

        let!(:collection) do
          client[TEST_COLL, write: {w: :majority}]
        end

        it_behaves_like 'an operation that is not retried'
      end

      context 'when the collection has write concern unacknowledged' do

        let!(:collection) do
          client[TEST_COLL, write: { w: 0 }]
        end

        it_behaves_like 'an operation that is not retried'
      end

      context 'when the collection has write concern not set' do

        let!(:collection) do
          client[TEST_COLL]
        end

        it_behaves_like 'an operation that is not retried'
      end
    end
  end

  context 'when the operation is insert_one' do

    let(:operation) do
      collection.insert_one(a:1)
    end

    let(:expectation) do
      check_collection.find(a: 1).count
    end

    let(:successful_retry_value) do
      1
    end

    let(:unsuccessful_retry_value) do
      0
    end

    it_behaves_like 'supported retryable writes'
  end

  context 'when the operation is update_one' do

    before do
      # Account for when the collection has unacknowledged write concern and use authorized_collection here.
      authorized_collection.insert_one(a:0)
    end

    let(:operation) do
      collection.update_one({ a: 0 }, { '$set' => { a: 1 } })
    end

    let(:expectation) do
      check_collection.find(a: 1).count
    end

    let(:successful_retry_value) do
      1
    end

    let(:unsuccessful_retry_value) do
      0
    end

    it_behaves_like 'supported retryable writes'
  end

  context 'when the operation is replace_one' do

    before do
      # Account for when the collection has unacknowledged write concern and use authorized_collection here.
      authorized_collection.insert_one(a:0)
    end

    let(:operation) do
      collection.replace_one({ a: 0 }, { a: 1 })
    end

    let(:expectation) do
      check_collection.find(a: 1).count
    end

    let(:successful_retry_value) do
      1
    end

    let(:unsuccessful_retry_value) do
      0
    end

    it_behaves_like 'supported retryable writes'
  end

  context 'when the operation is delete_one' do

    before do
      # Account for when the collection has unacknowledged write concern and use authorized_collection here.
      authorized_collection.insert_one(a:1)
    end

    let(:operation) do
      collection.delete_one(a:1)
    end

    let(:expectation) do
      check_collection.find(a: 1).count
    end

    let(:successful_retry_value) do
      0
    end

    let(:unsuccessful_retry_value) do
      1
    end

    it_behaves_like 'supported retryable writes'
  end

  context 'when the operation is find_one_and_update' do

    before do
      # Account for when the collection has unacknowledged write concern and use authorized_collection here.
      authorized_collection.insert_one(a:0)
    end

    let(:operation) do
      collection.find_one_and_update({ a: 0 }, { '$set' => { a: 1 } })
    end

    let(:expectation) do
      check_collection.find(a: 1).count
    end

    let(:successful_retry_value) do
      1
    end

    let(:unsuccessful_retry_value) do
      0
    end

    it_behaves_like 'supported retryable writes'
  end

  context 'when the operation is find_one_and_replace' do

    before do
      # Account for when the collection has unacknowledged write concern and use authorized_collection here.
      authorized_collection.insert_one(a:0)
    end

    let(:operation) do
      collection.find_one_and_replace({ a: 0 }, { a: 3 })
    end

    let(:expectation) do
      check_collection.find(a: 3).count
    end

    let(:successful_retry_value) do
      1
    end

    let(:unsuccessful_retry_value) do
      0
    end

    it_behaves_like 'supported retryable writes'
  end

  context 'when the operation is find_one_and_delete' do

    before do
      # Account for when the collection has unacknowledged write concern and use authorized_collection here.
      authorized_collection.insert_one(a:1)
    end

    let(:operation) do
      collection.find_one_and_delete({ a: 1 })
    end

    let(:expectation) do
      check_collection.find(a: 1).count
    end

    let(:successful_retry_value) do
      0
    end

    let(:unsuccessful_retry_value) do
      1
    end

    it_behaves_like 'supported retryable writes'
  end

  context 'when the operation is update_many' do

    before do
      # Account for when the collection has unacknowledged write concern and use authorized_collection here.
      authorized_collection.insert_one(a:0)
      authorized_collection.insert_one(a:0)
    end

    let(:operation) do
      collection.update_many({ a: 0 }, { '$set' => { a: 1 } })
    end

    let(:expectation) do
      check_collection.find(a: 1).count
    end

    let(:unsuccessful_retry_value) do
      0
    end

    it_behaves_like 'an operation that does not support retryable writes'
  end

  context 'when the operation is delete_many' do

    before do
      # Account for when the collection has unacknowledged write concern and use authorized_collection here.
      authorized_collection.insert_one(a:1)
      authorized_collection.insert_one(a:1)
    end

    let(:operation) do
      collection.delete_many(a: 1)
    end

    let(:expectation) do
      check_collection.find(a: 1).count
    end

    let(:unsuccessful_retry_value) do
      2
    end

    it_behaves_like 'an operation that does not support retryable writes'
  end

  context 'when the operation is a bulk write' do

    before do
      # Account for when the collection has unacknowledged write concern and use authorized_collection here.
      authorized_collection.insert_one(a: 1)
    end

    let(:operation) do
      collection.bulk_write([{ delete_one: { filter: { a: 1 } } },
                             { insert_one: { a: 1 } },
                             { insert_one: { a: 1 } }])
    end

    let(:expectation) do
      check_collection.find(a: 1).count
    end

    let(:successful_retry_value) do
      2
    end

    let(:unsuccessful_retry_value) do
      1
    end

    it_behaves_like 'supported retryable writes'
  end

  context 'when the operation is bulk write including delete_many' do

    before do
      # Account for when the collection has unacknowledged write concern and use authorized_collection here.
      authorized_collection.insert_one(a:1)
      authorized_collection.insert_one(a:1)
    end

    let(:operation) do
      collection.bulk_write([{ delete_many: { filter: { a: 1 } } }])
    end

    let(:expectation) do
      check_collection.find(a: 1).count
    end

    let(:unsuccessful_retry_value) do
      2
    end

    it_behaves_like 'an operation that does not support retryable writes'
  end

  context 'when the operation is bulk write including update_many' do

    before do
      # Account for when the collection has unacknowledged write concern and use authorized_collection here.
      authorized_collection.insert_one(a:0)
      authorized_collection.insert_one(a:0)
    end

    let(:operation) do
      collection.bulk_write([{ update_many: { filter: { a: 0 }, update: { a: 1 } } }])
    end

    let(:expectation) do
      check_collection.find(a: 1).count
    end

    let(:unsuccessful_retry_value) do
      0
    end

    it_behaves_like 'an operation that does not support retryable writes'
  end
end
