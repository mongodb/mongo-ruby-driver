# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Operation::Aggregate::Result do

  let(:description) do
    Mongo::Server::Description.new(
      double('description address'),
      { 'minWireVersion' => 0, 'maxWireVersion' => 2 }
    )
  end

  let(:result) do
    described_class.new(reply, description)
  end

  let(:cursor_id) { 0 }
  let(:documents) { [] }
  let(:flags) { [] }
  let(:starting_from) { 0 }

  let(:reply) do
    Mongo::Protocol::Reply.new.tap do |reply|
      reply.instance_variable_set(:@flags, flags)
      reply.instance_variable_set(:@cursor_id, cursor_id)
      reply.instance_variable_set(:@starting_from, starting_from)
      reply.instance_variable_set(:@number_returned, documents.size)
      reply.instance_variable_set(:@documents, documents)
    end
  end

  let(:aggregate) do
    [
      { '_id' => 'New York', 'totalpop' => 40270 },
      { '_id' => 'Berlin', 'totalpop' => 103056 }
    ]
  end

  describe '#cursor_id' do

    context 'when the result is not using a cursor' do

      let(:documents) do
        [{ 'result' => aggregate, 'ok' => 1.0 }]
      end

      it 'returns zero' do
        expect(result.cursor_id).to eq(0)
      end
    end

    context 'when the result is using a cursor' do

      let(:documents) do
        [{ 'cursor' => { 'id' => 15, 'ns' => 'test', 'firstBatch' => aggregate }, 'ok' => 1.0 }]
      end

      it 'returns the cursor id' do
        expect(result.cursor_id).to eq(15)
      end
    end
  end

  describe '#documents' do

    context 'when the result is not using a cursor' do

      let(:documents) do
        [{ 'result' => aggregate, 'ok' => 1.0 }]
      end

      it 'returns the documents' do
        expect(result.documents).to eq(aggregate)
      end
    end

    context 'when the result is using a cursor' do

      let(:documents) do
        [{ 'cursor' => { 'id' => 15, 'ns' => 'test', 'firstBatch' => aggregate }, 'ok' => 1.0 }]
      end

      it 'returns the documents' do
        expect(result.documents).to eq(aggregate)
      end
    end
  end
end
