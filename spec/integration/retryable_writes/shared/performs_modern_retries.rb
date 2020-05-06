module PerformsModernRetries
  shared_examples 'it performs modern retries' do
    include PrimarySocket

    # required for failCommand
    min_server_fcv '4.0'

    context 'for connection error' do
      before do
        client.use('admin').command(
          configureFailPoint: 'failCommand',
          mode: { times: times },
          data: {
            failCommands: [command_name],
            closeConnection: true,
          }
        )
      end

      context 'when error occurs once' do
        let(:times) { 1 }

        it 'retries and the operation and succeeds' do
          expect(Mongo::Logger.logger).to receive(:warn).once.with(/modern.*attempt 1/).and_call_original
          perform_operation
          expect(actual_result).to eq(successful_result)
        end
      end

      context 'when error occurs twice' do
        let(:times) { 2 }

        it 'retries and the operation and fails' do
          expect(Mongo::Logger.logger).to receive(:warn).once.with(/modern.*attempt 1/).and_call_original

          expect do
            perform_operation
          end.to raise_error(Mongo::Error::SocketError)
        end
      end
    end

    context 'for ETIMEDOUT' do
      # shorten socket timeout so these tests take less time to run
      let(:socket_timeout) { 1 }

      before do
        client.use('admin').command(
          configureFailPoint: 'failCommand',
          mode: { times: times },
          data: {
            failCommands: [command_name],
            blockConnection: true,
            blockTimeMS: 1500,
          }
        )
      end

      context 'when error occurs once' do
        let(:times) { 1 }

        it 'retries and the operation and succeeds' do
          expect(Mongo::Logger.logger).to receive(:warn).once.with(/modern.*attempt 1/).and_call_original
          perform_operation
          expect(actual_result).to eq(successful_result)
        end
      end

      context 'when error occurs twice' do
        let(:times) { 2 }

        it 'retries and the operation and fails' do
          expect(Mongo::Logger.logger).to receive(:warn).once.with(/modern.*attempt 1/).and_call_original

          expect do
            perform_operation
          end.to raise_error(Mongo::Error::SocketTimeoutError)
        end
      end
    end

    context 'on server versions >= 4.4' do
      min_server_fcv '4.3'
      # These tests will be implemented in a follow-up PR
    end

    context 'on server versions <= 4.4' do
      # max_server_fcv '4.2'

      context 'for OperationFailure with retryable code' do
        before do
          client.use('admin').command(
            configureFailPoint: 'failCommand',
            mode: { times: times },
            data: {
              failCommands: [command_name],
              errorCode: 91, # a retryable error code
            }
          )
        end

        context 'when error occurs once' do
          let(:times) { 1 }

          it 'retries and the operation and succeeds' do
            expect(Mongo::Logger.logger).to receive(:warn).once.with(/modern.*attempt 1/).and_call_original
            perform_operation
            expect(actual_result).to eq(successful_result)
          end
        end

        context 'when error occurs twice' do
          let(:times) { 2 }

          it 'retries and the operation and fails' do
            expect(Mongo::Logger.logger).to receive(:warn).once.with(/modern.*attempt 1/).and_call_original

            expect do
              perform_operation
            end.to raise_error(Mongo::Error::OperationFailure, /91/)
          end
        end
      end

      context 'for OperationFailure with non-retryable code' do
        before do
          client.use('admin').command(
            configureFailPoint: 'failCommand',
            mode: { times: times },
            data: {
              failCommands: [command_name],
              errorCode: 5, # a non-retryable error code
            }
          )
        end

        let(:times) { 1 }

        it 'raises the error' do
          expect(Mongo::Logger.logger).not_to receive(:warn)

          expect do
            perform_operation
          end.to raise_error(Mongo::Error::OperationFailure, /5/)
        end
      end
    end
  end
end
