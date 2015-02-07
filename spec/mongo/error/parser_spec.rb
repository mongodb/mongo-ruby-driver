require 'spec_helper'

describe Mongo::Error::Parser do

  describe '#parse' do

    let(:parser) do
      described_class.new(document)
    end

    context 'when the document contains an errmsg', mongo: 3.0 do

      let(:document) do
        { 'errmsg' => 'no such command: notacommand', 'code'=>59 }
      end

      it 'returns the message' do
        expect(parser.parse).to eq('no such command: notacommand (59)')
      end
    end

    context 'when the document contains writeErrors', mongo: 3.0 do

      let(:document) do
        { 'writeErrors' => [{ 'code' => 9, 'errmsg' => 'Unknown modifier: $st' }]}
      end

      it 'returns the message' do
        expect(parser.parse).to eq('Unknown modifier: $st (9)')
      end
    end

    context 'when the document contains $err', mongo: 2.6 do

      let(:document) do
        { '$err' => 'not authorized for query', 'code' => 13 }
      end

      it 'returns the message' do
        expect(parser.parse).to eq('not authorized for query (13)')
      end
    end

    context 'when the document contains writeConcernErrors', mongo: 3.0 do

      let(:document) do
        { 'writeConcernError' => [{ 'errmsg' => 'not authorized for query', 'code' => 13 }]}
      end

      it 'returns the message' do
        expect(parser.parse).to eq('not authorized for query (13)')
      end
    end
  end
end
