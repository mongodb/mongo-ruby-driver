require 'spec_helper'

describe Mongo::Error::Parser do

  describe '#message' do

    let(:parser) do
      described_class.new(document)
    end

    context 'when the document contains no error message' do

      let(:document) do
        { 'ok' => 1 }
      end

      it 'returns an empty string' do
        expect(parser.message).to be_empty
      end
    end

    context 'when the document contains an errmsg' do

      let(:document) do
        { 'errmsg' => 'no such command: notacommand', 'code' => 59 }
      end

      it 'returns the message' do
        expect(parser.message).to eq('no such command: notacommand (59)')
      end
    end

    context 'when the document contains writeErrors' do

      context 'when only a single error exists' do

        let(:document) do
          { 'writeErrors' => [{ 'code' => 9, 'errmsg' => 'Unknown modifier: $st' }]}
        end

        it 'returns the message' do
          expect(parser.message).to eq('Unknown modifier: $st (9)')
        end
      end

      context 'when multiple errors exist' do

        let(:document) do
          {
            'writeErrors' => [
              { 'code' => 9, 'errmsg' => 'Unknown modifier: $st' },
              { 'code' => 9, 'errmsg' => 'Unknown modifier: $bl' }
            ]
          }
        end

        it 'returns the messages concatenated' do
          expect(parser.message).to eq(
            'Unknown modifier: $st (9), Unknown modifier: $bl (9)'
          )
        end
      end
    end

    context 'when the document contains $err' do

      let(:document) do
        { '$err' => 'not authorized for query', 'code' => 13 }
      end

      it 'returns the message' do
        expect(parser.message).to eq('not authorized for query (13)')
      end
    end

    context 'when the document contains err' do

      let(:document) do
        { 'err' => 'not authorized for query', 'code' => 13 }
      end

      it 'returns the message' do
        expect(parser.message).to eq('not authorized for query (13)')
      end
    end

    context 'when the document contains a writeConcernError' do

      let(:document) do
        { 'writeConcernError' => { 'code' => 100, 'errmsg' => 'Not enough data-bearing nodes' } }
      end

      it 'returns the message' do
        expect(parser.message).to eq('Not enough data-bearing nodes (100)')
      end
    end
  end

  describe '#code' do
    let(:parser) do
      described_class.new(document)
    end

    context 'when document contains code and ok: 1' do
      let(:document) do
        { 'ok' => 1, 'errmsg' => 'not master', 'code' => 10107, 'codeName' => 'NotMaster' }
      end

      it 'returns nil' do
        expect(parser.code).to be nil
      end
    end

    context 'when document contains code and ok: 1.0' do
      let(:document) do
        { 'ok' => 1.0, 'errmsg' => 'not master', 'code' => 10107, 'codeName' => 'NotMaster' }
      end

      it 'returns nil' do
        expect(parser.code).to be nil
      end
    end

    context 'when document contains code' do
      let(:document) do
        { 'ok' => 0, 'errmsg' => 'not master', 'code' => 10107, 'codeName' => 'NotMaster' }
      end

      it 'returns the code' do
        expect(parser.code).to eq(10107)
      end

      context 'with legacy option' do
        let(:parser) do
          described_class.new(document, nil, legacy: true)
        end

        it 'returns nil' do
          expect(parser.code).to be nil
        end
      end
    end

    context 'when document does not contain code' do
      let(:document) do
        { 'ok' => 0, 'errmsg' => 'not master' }
      end

      it 'returns nil' do
        expect(parser.code).to eq(nil)
      end
    end

    context 'when the document contains a writeConcernError with a code' do

      let(:document) do
        { 'writeConcernError' => { 'code' => 100, 'errmsg' => 'Not enough data-bearing nodes' } }
      end

      it 'returns the code' do
        expect(parser.code).to eq(100)
      end
    end

    context 'when the document contains a writeConcernError without a code' do

      let(:document) do
        { 'writeConcernError' => { 'errmsg' => 'Not enough data-bearing nodes' } }
      end

      it 'returns nil' do
        expect(parser.code).to be nil
      end
    end
  end

  describe '#code_name' do
    let(:parser) do
      described_class.new(document)
    end

    context 'when document contains code name and ok: 1' do
      let(:document) do
        { 'ok' => 1, 'errmsg' => 'not master', 'code' => 10107, 'codeName' => 'NotMaster' }
      end

      it 'returns nil' do
        expect(parser.code_name).to be nil
      end
    end

    context 'when document contains code name and ok: 1.0' do
      let(:document) do
        { 'ok' => 1.0, 'errmsg' => 'not master', 'code' => 10107, 'codeName' => 'NotMaster' }
      end

      it 'returns nil' do
        expect(parser.code_name).to be nil
      end
    end

    context 'when document contains code name' do
      let(:document) do
        { 'ok' => 0, 'errmsg' => 'not master', 'code' => 10107, 'codeName' => 'NotMaster' }
      end

      it 'returns the code name' do
        expect(parser.code_name).to eq('NotMaster')
      end

      context 'with legacy option' do
        let(:parser) do
          described_class.new(document, nil, legacy: true)
        end

        it 'returns nil' do
          expect(parser.code_name).to be nil
        end
      end
    end

    context 'when document does not contain code name' do
      let(:document) do
        { 'ok' => 0, 'errmsg' => 'not master' }
      end

      it 'returns nil' do
        expect(parser.code_name).to eq(nil)
      end
    end

    context 'when the document contains a writeConcernError with a code' do

      let(:document) do
        { 'writeConcernError' => { 'code' => 100, 'codeName' => 'CannotSatisfyWriteConcern',
          'errmsg' => 'Not enough data-bearing nodes' } }
      end

      it 'returns the code name' do
        expect(parser.code_name).to eq('CannotSatisfyWriteConcern')
      end
    end

    context 'when the document contains a writeConcernError without a code' do

      let(:document) do
        { 'writeConcernError' => { 'code' => 100, 'errmsg' => 'Not enough data-bearing nodes' } }
      end

      it 'returns nil' do
        expect(parser.code_name).to be nil
      end
    end
  end

  describe '#document' do
    let(:parser) do
      described_class.new(document)
    end

    let(:document) do
      { 'ok' => 0, 'errmsg' => 'not master', 'code' => 10107, 'codeName' => 'NotMaster' }
    end

    it 'returns the document' do
      expect(parser.document).to eq(document)
    end
  end

  describe '#replies' do
    let(:parser) do
      described_class.new(document)
    end

    context 'when there are no replies' do
      let(:document) do
        { 'ok' => 0, 'errmsg' => 'not master', 'code' => 10107, 'codeName' => 'NotMaster' }
      end

      it 'returns nil' do
        expect(parser.replies).to eq(nil)
      end
    end
  end

  describe '#labels' do
    let(:parser) do
      described_class.new(document)
    end

    let(:document) do
      {
        'code' => 251,
        'codeName' => 'NoSuchTransaction',
        'errorLabels' => labels,
      }
    end

    context 'when there are no labels' do
      let(:labels) do
        []
      end

      it 'has the correct labels' do
        expect(parser.labels).to eq(labels)
      end
    end

    context 'when there are labels' do
      let(:labels) do
        [ Mongo::Error::TRANSIENT_TRANSACTION_ERROR_LABEL ]
      end

      it 'has the correct labels' do
        expect(parser.labels).to eq(labels)
      end
    end
  end

  describe '#wtimeout' do
    let(:parser) do
      described_class.new(document)
    end

    context 'when document contains wtimeout' do
      let(:document) do
        { 'ok' => 1, 'writeConcernError' => {
          'errmsg' => 'replication timed out', 'code' => 64,
          'errInfo' => {'wtimeout' => true}} }
      end

      it 'returns true' do
        expect(parser.wtimeout).to be true
      end
    end

    context 'when document does not contain wtimeout' do
      let(:document) do
        { 'ok' => 1, 'writeConcernError' => {
          'errmsg' => 'replication did not time out', 'code' => 55 }}
      end

      it 'returns nil' do
        expect(parser.wtimeout).to be nil
      end
    end
  end
end
