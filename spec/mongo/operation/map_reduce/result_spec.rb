# frozen_string_literal: true

require 'spec_helper'

describe Mongo::Operation::MapReduce::Result do
  let(:description) do
    Mongo::Server::Description.new(
      double('description address'),
      { 'minWireVersion' => 0, 'maxWireVersion' => 2 }
    )
  end

  let(:result) do
    described_class.new(reply, description)
  end

  let(:reply) do
    Mongo::Protocol::Reply.new.tap do |reply|
      reply.instance_variable_set(:@flags, [])
      reply.instance_variable_set(:@cursor_id, 0)
      reply.instance_variable_set(:@starting_from, 0)
      reply.instance_variable_set(:@number_returned, documents.size)
      reply.instance_variable_set(:@documents, documents)
    end
  end

  let(:aggregate) do
    [ { '_id' => 'Berlin', 'population' => 3_000_000 } ]
  end

  describe '#successful?' do
    context 'when the reply has no errors' do
      let(:documents) do
        [ { 'results' => aggregate, 'ok' => 1.0 } ]
      end

      it 'returns true' do
        expect(result).to be_successful
      end
    end

    context 'when the reply has a write concern error' do
      let(:documents) do
        [
          {
            'results' => aggregate,
            'ok' => 1.0,
            'writeConcernError' => {
              'code' => 64,
              'errmsg' => 'waiting replication timed out'
            }
          }
        ]
      end

      it 'returns false' do
        expect(result).not_to be_successful
      end
    end
  end
end
