# frozen_string_literal: true

require 'spec_helper'

describe Mongo::Retryable::WriteWorker do
  describe '#nro_write_with_retry' do
    context 'when session is nil' do
      let(:retryable) do
        authorized_client['write_worker_test']
      end

      let(:write_concern) do
        Mongo::WriteConcern.get(w: 0)
      end

      let(:write_worker) do
        described_class.new(retryable)
      end

      let(:context) do
        instance_double(Mongo::Operation::Context).tap do |context|
          allow(context).to receive(:session).and_return(nil)
        end
      end

      before do
        # We avoid actual execution of the operation to speed up and simplify
        # the spec.
        allow(write_worker).to receive(:legacy_write_with_retry).and_return(nil)
      end

      it 'does not raise' do
        expect do
          write_worker.nro_write_with_retry(write_concern, context: context)
        end.not_to raise_error
      end
    end
  end
end
