# frozen_string_literal: true
# rubocop:todo all

module PerformsNoRetries
  shared_examples 'it performs no retries' do
    # required for failCommand
    min_server_fcv '4.0'

    context 'for connection error' do
      before do
        client.use('admin').command(
          configureFailPoint: 'failCommand',
          mode: { times: 1 },
          data: {
            failCommands: [command_name],
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
      min_server_fcv '4.4'

      # shorten socket timeout so these tests take less time to run
      let(:socket_timeout) { 1 }

      before do
        client.use('admin').command(
          configureFailPoint: 'failCommand',
          mode: { times: 1 },
          data: {
            failCommands: [command_name],
            blockConnection: true,
            blockTimeMS: 1100,
          }
        )
      end

      it 'does not retry the operation' do
        expect(Mongo::Logger.logger).not_to receive(:warn)

        expect do
          perform_operation
        end.to raise_error(Mongo::Error::SocketTimeoutError)
      end

      after do
        # Assure that the server has completed the operation before moving
        # on to the next test.
        sleep 1
      end
    end

    context 'on server versions >= 4.4' do
      min_server_fcv '4.4'
      # These tests will be implemented in a follow-up PR
    end

    context 'on server versions <= 4.4' do
      max_server_fcv '4.2'

      context 'for OperationFailure with retryable code' do
        before do
          client.use('admin').command(
            configureFailPoint: 'failCommand',
            mode: { times: 1 },
            data: {
              failCommands: [command_name],
              errorCode: 91, # a retryable error code
            }
          )
        end
        it 'does not retry the operation' do
          expect(Mongo::Logger.logger).not_to receive(:warn)

          expect do
            perform_operation
          end.to raise_error(Mongo::Error::OperationFailure, /91/)
        end
      end

      context 'for OperationFailure with non-retryable code' do
        before do
          client.use('admin').command(
            configureFailPoint: 'failCommand',
            mode: { times: 1 },
            data: {
              failCommands: [command_name],
              errorCode: 5, # a non-retryable error code
            }
          )
        end

        it 'does not retry the operation' do
          expect(Mongo::Logger.logger).not_to receive(:warn)

          expect do
            perform_operation
          end.to raise_error(Mongo::Error::OperationFailure, /5/)
        end
      end
    end
  end
end
