require 'spec_helper'

describe Mongo::Protocol::Reply do

  let(:length)      { 78 }
  let(:request_id)  { 0 }
  let(:response_to) { 0 }
  let(:op_code)     { 1 }
  let(:flags)       { 0 }
  let(:start)       { 0 }
  let(:n_returned)  { 2 }
  let(:cursor_id)   { 999_999 }
  let(:doc)         { { 'name' => 'Tyler' } }
  let(:documents)   { [doc] * 2 }

  let(:header) do
    [length, request_id, response_to, op_code].pack('l<l<l<l<')
  end

  let(:data) do
    data =  [flags].pack('l<')
    data << [cursor_id].pack('q<')
    data << [start].pack('l<')
    data << [n_returned].pack('l<')
    data << documents.map(&:to_bson).join
  end

  let(:io)    { StringIO.new(header + data) }
  let(:reply) { described_class.deserialize(io) }

  describe '#==' do

    context 'when the other is a reply' do

      context 'when the fields are equal' do
        let(:other) do
          other = described_class.allocate
          other.instance_variable_set(:@flags, [])
          other.instance_variable_set(:@number_returned, n_returned)
          other.instance_variable_set(:@cursor_id, cursor_id)
          other.instance_variable_set(:@documents, documents)
          other.instance_variable_set(:@starting_from, start)
          other
        end

        it 'returns true' do
          expect(reply).to eq(other)
        end
      end

      context 'when fields are not equal' do
        let(:other) do
          other = described_class.allocate
          other.instance_variable_set(:@flags, [:await_capable])
          other.instance_variable_set(:@number_returned, n_returned)
          other.instance_variable_set(:@cursor_id, cursor_id)
          other.instance_variable_set(:@documents, documents)
          other.instance_variable_set(:@starting_from, start)
          other
        end

        it 'returns false' do
          expect(reply).not_to eq(other)
        end
      end
    end

    context 'when the other is not a reply' do
      let(:other) do
        expect(message).not_to eq('test')
      end
    end
  end

  describe '#hash' do
    let(:values) do
      reply.send(:fields).map do |field|
        reply.instance_variable_get(field[:name])
      end
    end

    it 'returns a hash of the field values' do
      expect(reply.hash).to eq(values.hash)
    end
  end

  describe '#deserialize' do

    context 'when the message size is greater than the max message size' do

      let(:length) { Mongo::Protocol::Message::MAX_MESSAGE_SIZE + 1 }

      it 'raises a max message size error' do
        expect {
          reply
        }.to raise_error(Mongo::Error::MaxMessageSize)
      end
    end

    context 'when the max message size is nil' do

      let(:reply) { described_class.deserialize(io, nil) }

      let(:length) { Mongo::Protocol::Message::MAX_MESSAGE_SIZE + 1 }

      it 'uses the default max message size for comparison' do
        expect {
          reply
        }.to raise_error(Mongo::Error::MaxMessageSize)
      end
    end

    describe 'response flags' do

      context 'no flags' do
        it 'sets no flags' do
          expect(reply.flags).to be_empty
        end
      end

      context 'cursor not found' do
        let(:flags) { 1 }
        it 'sets the cursor not found flag' do
          expect(reply.flags).to eq([:cursor_not_found])
        end
      end

      context 'query failure' do
        let(:flags) { 2 }
        it 'sets the query failure flag' do
          expect(reply.flags).to eq([:query_failure])
        end
      end

      context 'shard config stale' do
        let(:flags) { 4 }
        it 'sets the shard config stale flag' do
          expect(reply.flags).to eq([:shard_config_stale])
        end
      end

      context 'await capable' do
        let(:flags) { 8 }
        it 'sets the await capable flag' do
          expect(reply.flags).to eq([:await_capable])
        end
      end

      context 'multiple flags' do
        let(:flags) { 10 }
        it 'sets multiple flags' do
          expect(reply.flags).to include(:query_failure, :await_capable)
        end
      end
    end

    describe 'cursor id' do
      it 'sets the cursor id attribute' do
        expect(reply.cursor_id).to eq(cursor_id)
      end
    end

    describe 'starting from' do
      it 'sets the starting from attribute' do
        expect(reply.starting_from).to eq(start)
      end
    end

    describe 'number returned' do
      it 'sets the number returned attribute' do
        expect(reply.number_returned).to eq(n_returned)
      end
    end

    describe 'documents' do
      it 'sets the documents attribute' do
        expect(reply.documents).to eq(documents)
      end
    end
  end
end
