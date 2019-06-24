require 'spec_helper'

describe Mongo::Error::Parser do
  let(:parser) do
    described_class.new(document)
  end

  describe '#message' do

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

    context 'when both top level code and write concern code are present' do

      let(:document) do
        { 'ok' => 0,
          'errmsg' => 'not master', 'code' => 10107, 'codeName' => 'NotMaster',
          'writeConcernError' => {
            'code' => 100, 'errmsg' => 'Not enough data-bearing nodes' } }
      end

      it 'returns top level code' do
        expect(parser.code).to eq(10107)
      end
    end
  end

  describe '#code_name' do

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

    context 'when both top level code and write concern code are present' do

      let(:document) do
        { 'ok' => 0,
          'errmsg' => 'not master', 'code' => 10107, 'codeName' => 'NotMaster',
          'writeConcernError' => {
            'code' => 100, 'errmsg' => 'Not enough data-bearing nodes' } }
      end

      it 'returns top level code' do
        expect(parser.code_name).to eq('NotMaster')
      end
    end
  end

  describe '#write_concern_error?' do
    context 'there is a write concern error' do

      let(:document) do
        { 'ok' => 1,
          'writeConcernError' => {
            'code' => 100, 'errmsg' => 'Not enough data-bearing nodes' } }
      end

      it 'is true' do
        expect(parser.write_concern_error?).to be true
      end
    end

    context 'there is no write concern error' do

      let(:document) do
        { 'ok' => 0,
          'errmsg' => 'not master', 'code' => 10107, 'codeName' => 'NotMaster',
        }
      end

      it 'is false' do
        expect(parser.write_concern_error?).to be false
      end
    end

    context 'there is a top level error and write concern error' do

      let(:document) do
        { 'ok' => 0,
          'errmsg' => 'not master', 'code' => 10107, 'codeName' => 'NotMaster',
          'writeConcernError' => {
            'code' => 100, 'errmsg' => 'Not enough data-bearing nodes' } }
      end

      it 'is true' do
        expect(parser.write_concern_error?).to be true
      end
    end
  end

  describe '#write_concern_error_code' do
    context 'there is a write concern error' do

      let(:document) do
        { 'ok' => 1,
          'writeConcernError' => {
            'code' => 100, 'errmsg' => 'Not enough data-bearing nodes' } }
      end

      it 'is true' do
        expect(parser.write_concern_error_code).to eq(100)
      end
    end

    context 'there is no write concern error' do

      let(:document) do
        { 'ok' => 0,
          'errmsg' => 'not master', 'code' => 10107, 'codeName' => 'NotMaster',
        }
      end

      it 'is nil' do
        expect(parser.write_concern_error_code).to be nil
      end
    end

    context 'there is a top level error and write concern error' do

      let(:document) do
        { 'ok' => 0,
          'errmsg' => 'not master', 'code' => 10107, 'codeName' => 'NotMaster',
          'writeConcernError' => {
            'code' => 100, 'errmsg' => 'Not enough data-bearing nodes' } }
      end

      it 'is true' do
        expect(parser.write_concern_error_code).to eq(100)
      end
    end
  end

  describe '#write_concern_error_code_name' do
    context 'there is a write concern error' do

      let(:document) do
        { 'ok' => 1,
          'writeConcernError' => {
            'code' => 100, 'codeName' => 'SomeCodeName',
              'errmsg' => 'Not enough data-bearing nodes' } }
      end

      it 'is the code name' do
        expect(parser.write_concern_error_code_name).to eq('SomeCodeName')
      end
    end

    context 'there is no write concern error' do

      let(:document) do
        { 'ok' => 0,
          'errmsg' => 'not master', 'code' => 10107, 'codeName' => 'NotMaster',
        }
      end

      it 'is nil' do
        expect(parser.write_concern_error_code_name).to be nil
      end
    end

    context 'there is a top level error and write concern error' do

      let(:document) do
        { 'ok' => 0,
          'errmsg' => 'not master', 'code' => 10107, 'codeName' => 'NotMaster',
          'writeConcernError' => {
            'code' => 100, 'codeName' => 'SomeCodeName',
              'errmsg' => 'Not enough data-bearing nodes' } }
      end

      it 'is the code name' do
        expect(parser.write_concern_error_code_name).to eq('SomeCodeName')
      end
    end
  end

  describe '#document' do

    let(:document) do
      { 'ok' => 0, 'errmsg' => 'not master', 'code' => 10107, 'codeName' => 'NotMaster' }
    end

    it 'returns the document' do
      expect(parser.document).to eq(document)
    end
  end

  describe '#replies' do

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
        %w(TransientTransactionError)
      end

      it 'has the correct labels' do
        expect(parser.labels).to eq(labels)
      end
    end
  end

  describe '#wtimeout' do

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
