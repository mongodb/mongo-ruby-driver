# frozen_string_literal: true
# rubocop:todo all

module CSOT
  module Examples
    # expects the following values to be available:
    #  `op` -- an instance of an OpMsgBase subclass
    def self.included(example_context)
      example_context.shared_examples 'mock CSOT environment' do
        # Linting freaks out because of the doubles used in these specs.
        require_no_linting

        let(:message) { op.send(:message, connection) }

        let(:body) { message.documents.first }

        let(:cursor_type) { nil }
        let(:timeout_mode) { nil }
        let(:remaining_timeout_sec) { nil }
        let(:minimum_round_trip_time) { 0 }
        let(:view_options) { {} }
        let(:max_await_time_ms) { nil }

        let(:view) do
          instance_double(Mongo::Collection::View).tap do |view|
            allow(view).to receive(:cursor_type).and_return(cursor_type)
            allow(view).to receive(:timeout_mode).and_return(timeout_mode)
            allow(view).to receive(:options).and_return(view_options)
            allow(view).to receive(:max_await_time_ms).and_return(max_await_time_ms)
          end
        end

        let(:context) do
          Mongo::Operation::Context.new(view: view).tap do |context|
            allow(context).to receive(:remaining_timeout_sec).and_return(remaining_timeout_sec)
            allow(context).to receive(:timeout?).and_return(!remaining_timeout_sec.nil?)
          end
        end

        let(:server) do
          instance_double(Mongo::Server).tap do |server|
            allow(server).to receive(:minimum_round_trip_time).and_return(minimum_round_trip_time)
          end
        end

        let(:address) { Mongo::Address.new('127.0.0.1') }

        let(:description) do
          Mongo::Server::Description.new(
            address, { Mongo::Operation::Result::OK => 1 }
          )
        end

        let(:features) do
          Mongo::Server::Description::Features.new(
            Mongo::Server::Description::Features::DRIVER_WIRE_VERSIONS,
            address
          )
        end

        let(:connection) do
          instance_double(Mongo::Server::Connection).tap do |conn|
            allow(conn).to receive(:server).and_return(server)
            allow(conn).to receive(:description).and_return(description)
            allow(conn).to receive(:features).and_return(features)
          end
        end

        before do
          # context is normally set when calling `execute` on the operation,
          # but since we're not doing that, we have to tell the operation
          # what the context is.
          op.context = context
        end
      end

      example_context.shared_examples 'a CSOT-compliant OpMsg subclass' do
        include_examples 'mock CSOT environment'

        context 'when no timeout_ms set' do
          it 'does not set maxTimeMS' do
            expect(body.key?(:maxTimeMS)).to be false
          end
        end

        context 'when there is enough time to send the message' do
          # Ten seconds remaining
          let(:remaining_timeout_sec) { 10 }

          # One second RTT
          let(:minimum_round_trip_time) { 1 }

          it 'sets the maxTimeMS' do
            # Nine seconds
            expect(body[:maxTimeMS]).to eq(9_000)
          end
        end

        context 'when there is not enough time to send the message' do
          # Ten seconds remaining
          let(:remaining_timeout_sec) { 0.1 }

          # One second RTT
          let(:minimum_round_trip_time) { 1 }

          it 'fails with an exception' do
            expect { message }.to raise_error(Mongo::Error::TimeoutError)
          end
        end
      end
    end
  end
end
