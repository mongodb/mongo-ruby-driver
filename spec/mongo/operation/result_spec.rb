require 'spec_helper'

describe Mongo::Operation::Result do

  let(:result) do
    described_class.new(reply)
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

  describe '#acknowledged?' do

    context 'when the reply is for a read command' do

      let(:documents) do
        [{ 'ismaster' => true, 'ok' => 1.0 }]
      end

      it 'returns true' do
        expect(result).to be_acknowledged
      end
    end

    context 'when the reply is for a write command' do

      context 'when the command was acknowledged' do

        let(:documents) do
          [{ "ok" => 1, "n" => 2 }]
        end

        it 'returns true' do
          expect(result).to be_acknowledged
        end
      end

      context 'when the command was not acknowledged' do

        let(:reply) { nil }

        it 'returns false' do
          expect(result).to_not be_acknowledged
        end
      end
    end
  end

  describe '#cursor_id' do

    context 'when the reply exists' do

      let(:cursor_id) { 5 }

      it 'delegates to the reply' do
        expect(result.cursor_id).to eq(5)
      end
    end

    context 'when the reply does not exist' do

      let(:reply) { nil }

      it 'returns zero' do
        expect(result.cursor_id).to eq(0)
      end
    end
  end

  describe '#documents' do

    context 'when the result is for a command' do

      context 'when a reply is received' do

        let(:documents) do
          [{ "ok" => 1, "n" => 2 }]
        end

        it 'returns the documents' do
          expect(result.documents).to eq(documents)
        end
      end

      context 'when a reply is not received' do

        let(:reply) { nil }

        it 'returns an empty array' do
          expect(result.documents).to be_empty
        end
      end
    end
  end

  describe '#each' do

    let(:documents) do
      [{ "ok" => 1, "n" => 2 }]
    end

    context 'when a block is given' do

      it 'yields to each document' do
        result.each do |document|
          expect(document).to eq(documents.first)
        end
      end
    end

    context 'when no block is given' do

      it 'returns an enumerator' do
        expect(result.each).to be_a(Enumerator)
      end
    end
  end

  describe '#initialize' do

    it 'sets the replies' do
      expect(result.replies).to eq([ reply ])
    end
  end

  describe '#returned_count' do

    context 'when the reply is for a read command' do

      let(:documents) do
        [{ 'ismaster' => true, 'ok' => 1.0 }]
      end

      it 'returns the number returned' do
        expect(result.returned_count).to eq(1)
      end
    end

    context 'when the reply is for a write command' do

      context 'when the write is acknowledged' do

        let(:documents) do
          [{ "ok" => 1, "n" => 2 }]
        end

        it 'returns the number returned' do
          expect(result.returned_count).to eq(1)
        end
      end

      context 'when the write is not acknowledged' do

        let(:reply) { nil }

        it 'returns zero' do
          expect(result.returned_count).to eq(0)
        end
      end
    end
  end

  describe '#successful?' do

    context 'when the reply is for a read command' do

      let(:documents) do
        [{ 'ismaster' => true, 'ok' => 1.0 }]
      end

      it 'returns true' do
        expect(result).to be_successful
      end
    end

    context 'when the reply is for a query' do

      context 'when the query has no errors' do

        let(:documents) do
          [{ 'field' => 'name' }]
        end

        it 'returns true' do
          expect(result).to be_successful
        end
      end

      context 'when the query has errors' do

        let(:documents) do
          [{ '$err' => 'not authorized for query on test.system.namespaces', 'code'=> 16550 }]
        end

        it 'returns false' do
          expect(result).to_not be_successful
        end
      end

      context 'when the query reply has the cursor_not_found flag set' do

        let(:flags) do
          [ :cursor_not_found ]
        end

        let(:documents) do
          []
        end

        it 'returns false' do
          expect(result).to_not be_successful
        end
      end
    end

    context 'when the reply is for a write command' do

      context 'when the write is acknowledged' do

        context 'when ok is 1' do

          let(:documents) do
            [{ "ok" => 1, "n" => 2 }]
          end

          it 'returns true' do
            expect(result).to be_successful
          end
        end

        context 'when ok is not 1' do

          let(:documents) do
            [{ "ok" => 0, "n" => 0 }]
          end

          it 'returns false' do
            expect(result).to_not be_successful
          end
        end
      end

      context 'when the write is not acknowledged' do

        let(:reply) { nil }

        it 'returns true' do
          expect(result).to be_successful
        end
      end
    end

    context 'when there is a write concern error' do
      let(:documents) do
        [{'ok' => 1.0, 'writeConcernError' => {
          'code' => 91, 'errmsg' => 'Replication is being shut down'}}]
      end

      it 'is false' do
        expect(result).not_to be_successful
      end
    end
  end

  describe '#written_count' do

    context 'when the reply is for a read command' do

      let(:documents) do
        [{ 'ismaster' => true, 'ok' => 1.0 }]
      end

      it 'returns the number written' do
        expect(result.written_count).to eq(0)
      end
    end

    context 'when the reply is for a write command' do

      let(:documents) do
        [{ "ok" => 1, "n" => 2 }]
      end

      it 'returns the number written' do
        expect(result.written_count).to eq(2)
      end
    end
  end

  context 'when there is a top-level Result class defined' do
    let(:client) do
      new_local_client(SpecConfig.instance.addresses, SpecConfig.instance.test_options)
    end

    before do
      class Result
        def get_result(client)
          client.database.command(:ping => 1)
        end
      end
    end

    let(:result) do
      Result.new.get_result(client)
    end

    it 'uses the Result class of the operation' do
      expect(result).to be_a(Mongo::Operation::Result)
    end
  end

  describe '#validate!' do

    context 'when there is a write concern error' do
      let(:documents) do
        [{'ok' => 1.0, 'writeConcernError' => {
          'code' => 91, 'errmsg' => 'Replication is being shut down'}}]
      end

      it 'raises OperationFailure' do
        expect do
          result.validate!
        end.to raise_error(Mongo::Error::OperationFailure, /Replication is being shut down \(91\)/)
      end
    end
  end
end
