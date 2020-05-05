require 'spec_helper'

describe 'legacy retryable writes integration tests' do
  include PrimarySocket
  require_topology :replica_set

  let(:client) { authorized_client.with(retry_writes: false) }

  before do
    client['test'].drop
  end

  context 'when error is an IOError' do
    let(:error) { IOError.new('io error') }

    before do
      allow(primary_socket).to receive(:do_write).and_raise(error.dup)
    end

    # legacy retry writes do not retry socket errors
    it 'does not retry the operation' do
      expect do
        client['test'].insert_one(_id: 1)
      end.to raise_error(Mongo::Error::SocketError, /io error/)
    end
  end

  context 'when error is an ETIMEDOUT' do
    let(:error) { Errno::ETIMEDOUT.new('timeout') }

    before do
      allow(primary_socket).to receive(:do_write).and_raise(error.dup)
    end

    # legacy retry writes do not retry socket errors
    it 'does not retry the operation' do
      expect do
        client['test'].insert_one(_id: 1)
      end.to raise_error(Mongo::Error::SocketTimeoutError, /timeout/)
    end
  end

  context 'when error is an operation failure' do
    # 4.0 required for failCommand
    min_server_fcv '4.0'

    context 'with a retryable code' do
      before do
        client.use('admin').command(
          configureFailPoint: 'failCommand',
          mode: { times: 1 },
          data: { failCommands: ['insert'], errorCode: 91 }
        )
      end

      it 'retries the operation' do
        expect(Mongo::Logger.logger).to receive(:warn).once.and_call_original

        result = client['test'].insert_one(_id: 1)
        expect(result).to be_ok
      end
    end

    context 'with a non-retryable code' do
      before do
        client.use('admin').command(
          configureFailPoint: 'failCommand',
          mode: { times: 1 },
          data: { failCommands: ['insert'], errorCode: 5 }
        )
      end

      it 'retries the operation' do
        expect do
          client['test'].insert_one(_id: 1)
        end.to raise_error(Mongo::Error::OperationFailure)
      end
    end
  end
end
