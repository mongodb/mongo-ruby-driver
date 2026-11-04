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
      session_id: { 'id' => session_id_binary },
      tracing_transaction_key: nil
    ).tap do |s|
      # Behaves like the real accessor: what the tracer stores, it reads back.
      allow(s).to receive(:tracing_transaction_key=) do |value|
        allow(s).to receive(:tracing_transaction_key).and_return(value)
      end
    end
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
end
