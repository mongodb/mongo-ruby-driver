# frozen_string_literal: true

require 'spec_helper'

require 'opentelemetry'

describe Mongo::Tracing::OpenTelemetry::Tracer do
  let(:otel_tracer) { double('OpenTelemetry::Trace::Tracer') }
  let(:tracer) { described_class.new(enabled: true, otel_tracer: otel_tracer) }

  let(:session_uuid) { 'de124f0e-b9a8-4bfc-8f4e-6d9c8f0a3c1d' }
  let(:session_id_binary) { double('BSON::Binary', to_uuid: session_uuid) }
  let(:session) do
    instance_double(
      Mongo::Session,
      implicit?: false,
      in_transaction?: true,
      txn_num: 42,
      session_id: { 'id' => session_id_binary }
    )
  end

  describe '#transaction_map_key' do
    context 'when the session is nil' do
      it 'returns nil' do
        expect(tracer.transaction_map_key(nil)).to be_nil
      end
    end

    context 'when the session is implicit' do
      let(:session) { instance_double(Mongo::Session, implicit?: true) }

      it 'returns nil' do
        expect(tracer.transaction_map_key(session)).to be_nil
      end
    end

    context 'when the session is not in a transaction' do
      let(:session) { instance_double(Mongo::Session, implicit?: false, in_transaction?: false) }

      it 'returns nil' do
        expect(tracer.transaction_map_key(session)).to be_nil
      end
    end

    context 'when the session is in a transaction' do
      it 'combines the session UUID and the transaction number' do
        expect(tracer.transaction_map_key(session)).to eq("#{session_uuid}-42")
      end

      it 'formats the session UUID once across repeated calls' do
        expect(session_id_binary).to receive(:to_uuid).once.and_return(session_uuid)

        3.times { tracer.transaction_map_key(session) }
      end

      it 'computes a new key when the transaction number changes' do
        expect(tracer.transaction_map_key(session)).to eq("#{session_uuid}-42")

        allow(session).to receive(:txn_num).and_return(43)
        expect(tracer.transaction_map_key(session)).to eq("#{session_uuid}-43")
      end
    end
  end

  describe '#parent_context_for' do
    let(:operation_context) { instance_double(Mongo::Operation::Context, session: session) }

    context 'when the session is in a transaction' do
      it 'returns the transaction context' do
        context = double('OpenTelemetry::Context')
        tracer.transaction_context_map["#{session_uuid}-42"] = context

        expect(tracer.parent_context_for(operation_context)).to eq(context)
      end

      it 'returns nil when the transaction has no stored context' do
        expect(tracer.parent_context_for(operation_context)).to be_nil
      end
    end

    context 'when the session is not in a transaction' do
      let(:session) { instance_double(Mongo::Session, implicit?: true) }

      it 'returns nil without formatting a session key' do
        expect(session).not_to receive(:session_id)

        expect(tracer.parent_context_for(operation_context)).to be_nil
      end
    end
  end
end
