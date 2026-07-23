# frozen_string_literal: true

module PerformsNoRetries
  shared_examples 'it performs no retries' do
    context 'for connection error' do
      before do
        client.use('admin').command(
          configureFailPoint: 'failCommand',
          mode: { times: 1 },
          data: {
            failCommands: [ command_name ],
            closeConnection: true,
          }
        )
      end

      it 'does not retry the operation' do
        expect(Mongo::Logger.logger).not_to receive(:warn)

        expect do
          perform_operation
        end.to raise_error(Mongo::Error::SocketError)
      end
    end

    context 'for ETIMEDOUT' do
      # shorten socket timeout so these tests take less time to run
      let(:socket_timeout) { 1 }

      before do
        client.use('admin').command(
          configureFailPoint: 'failCommand',
          mode: { times: 1 },
          data: {
            failCommands: [ command_name ],
            blockConnection: true,
            blockTimeMS: 1100,
          }
        )
      end

      after do
        # Assure that the server has completed the operation before moving
        # on to the next test.
        sleep 1
      end

      it 'does not retry the operation' do
        expect(Mongo::Logger.logger).not_to receive(:warn)

        expect do
          perform_operation
        end.to raise_error(Mongo::Error::SocketTimeoutError)
      end
    end
  end
end
