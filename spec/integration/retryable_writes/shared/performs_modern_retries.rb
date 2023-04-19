# frozen_string_literal: true
# rubocop:todo all

require_relative './adds_diagnostics'

module PerformsModernRetries
  shared_examples 'it performs modern retries' do

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

        it 'retries and the operation succeeds' do
          expect(Mongo::Logger.logger).to receive(:warn).once.with(/modern.*attempt 1/).and_call_original
          perform_operation
          expect(actual_result).to eq(expected_successful_result)
        end
      end

      context 'when error occurs twice' do
        let(:times) { 2 }

        it 'retries and the operation and fails' do
          expect(Mongo::Logger.logger).to receive(:warn).once.with(/modern.*attempt 1/).and_call_original

          expect do
            perform_operation
          end.to raise_error(Mongo::Error::SocketError)

          expect(actual_result).to eq(expected_failed_result)
        end

        it_behaves_like 'it adds diagnostics'
      end
    end

    context 'for ETIMEDOUT' do
      # blockConnection option in failCommand was introduced in
      # server version 4.4
      min_server_fcv '4.4'

      # shorten socket timeout so these tests take less time to run
      let(:socket_timeout) { 1 }

      before do
        client.use('admin').command(
          configureFailPoint: 'failCommand',
          mode: { times: times },
          data: {
            failCommands: [command_name],
            blockConnection: true,
            blockTimeMS: 1100,
          }
        )
      end

      context 'when error occurs once' do
        let(:times) { 1 }

        it 'retries and the operation succeeds' do
          expect(Mongo::Logger.logger).to receive(:warn).once.with(/modern.*attempt 1/).and_call_original
          perform_operation
          expect(actual_result).to eq(expected_successful_result)
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

        it_behaves_like 'it adds diagnostics'

        after do
          # Assure that the server has completed the operation before moving
          # on to the next test.
          sleep 1
        end
      end
    end

    context 'on server versions >= 4.4' do
      min_server_fcv '4.4'

      context 'for OperationFailure with RetryableWriteError label' do
        before do
          client.use('admin').command(
            configureFailPoint: 'failCommand',
            mode: { times: times },
            data: {
              failCommands: [command_name],
              errorCode: 5, # normally NOT a retryable error code
              errorLabels: ['RetryableWriteError']
            }
          )
        end

        context 'when error occurs once' do
          let(:times) { 1 }

          it 'retries and the operation and succeeds' do
            expect(Mongo::Logger.logger).to receive(:warn).once.with(/modern.*attempt 1/).and_call_original
            perform_operation
            expect(actual_result).to eq(expected_successful_result)
          end
        end

        context 'when error occurs twice' do
          let(:times) { 2 }

          it 'retries the operation and fails' do
            expect(Mongo::Logger.logger).to receive(:warn).once.with(/modern.*attempt 1/).and_call_original

            expect do
              perform_operation
            end.to raise_error(Mongo::Error::OperationFailure, /5/)

            expect(actual_result).to eq(expected_failed_result)
          end

          it_behaves_like 'it adds diagnostics'
        end
      end

      context 'for OperationFailure without RetryableWriteError label' do
        before do
          client.use('admin').command(
            configureFailPoint: 'failCommand',
            mode: { times: 1 },
            data: {
              failCommands: [command_name],
              errorCode: 91, # normally a retryable error code
              errorLabels: [],
            }
          )
        end

        it 'raises the error' do
          expect(Mongo::Logger.logger).not_to receive(:warn)

          expect do
            perform_operation
          end.to raise_error(Mongo::Error::OperationFailure, /91/)

          expect(actual_result).to eq(expected_failed_result)
        end
      end
    end

    context 'on server versions < 4.4' do
      max_server_fcv '4.2'

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

          it 'retries and the operation succeeds' do
            expect(Mongo::Logger.logger).to receive(:warn).once.with(/modern.*attempt 1/).and_call_original
            perform_operation
            expect(actual_result).to eq(expected_successful_result)
          end
        end

        context 'when error occurs twice' do
          let(:times) { 2 }

          it 'retries and the operation and fails' do
            expect(Mongo::Logger.logger).to receive(:warn).once.with(/modern.*attempt 1/).and_call_original

            expect do
              perform_operation
            end.to raise_error(Mongo::Error::OperationFailure, /91/)

            expect(actual_result).to eq(expected_failed_result)
          end

          it_behaves_like 'it adds diagnostics'
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

          expect(actual_result).to eq(expected_failed_result)
        end
      end
    end
  end
end
