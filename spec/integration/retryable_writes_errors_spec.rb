# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

describe 'Retryable writes errors tests' do

  let(:client) do
    authorized_client.with(retry_writes: true)
  end

  let(:collection) do
    client['retryable-writes-error-spec']
  end

  context 'when the storage engine does not support retryable writes but the server does' do
    require_mmapv1
    min_server_fcv '3.6'
    require_topology :replica_set, :sharded

    before do
      collection.delete_many
    end

    context 'when a retryable write is attempted' do
      it 'raises an actionable error message' do
        expect {
          collection.insert_one(a:1)
        }.to raise_error(Mongo::Error::OperationFailure, /This MongoDB deployment does not support retryable writes. Please add retryWrites=false to your connection string or use the retry_writes: false Ruby client option/)
        expect(collection.find.count).to eq(0)
      end
    end
  end

  context "when encountering a WriteConcernError with a RetryableWriteError label" do
    require_topology :replica_set

    let(:failpoint1) do
      {
        configureFailPoint: "failCommand",
        mode: { times: 1 },
        data: {
          writeConcernError: {
            code: 91,
            errorLabels: ["RetryableWriteError"],
          },
          failCommands: ["insert"],
        }
      }
    end

    let(:failpoint2) do
      {
        configureFailPoint: "failCommand",
        mode: { times: 1 },
        data: {
          errorCode: 10107,
          errorLabels: ["RetryableWriteError", "NoWritesPerformed"],
          failCommands: ["insert"],
        },
      }
    end

    let(:subscriber) { Mrss::EventSubscriber.new }

    before do
      authorized_client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
      authorized_client.use(:admin).command(failpoint1)

      expect(authorized_collection).to receive(:retry_write).once.and_wrap_original do |m, *args, &block|
        expect(args.first.code).to eq(91)
        authorized_client.use(:admin).command(failpoint2)
        m.call(*args, &block)
      end
    end

    after do
      authorized_client.use(:admin).command({
        configureFailPoint: "failCommand",
        mode: "off",
      })
    end

    it "returns the original error" do
      expect do
        authorized_collection.insert_one(x: 1)
      end.to raise_error(Mongo::Error::OperationFailure, /\[91\]/)
    end
  end
end
