# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::BulkWrite::Result do
  let(:results_document) do
    {'n_inserted' => 2, 'n' => 3, 'inserted_ids' => [1, 2]}
  end

  let(:subject) { described_class.new(results_document, true) }

  describe 'construction' do
    it 'works' do
      expect(subject).to be_a(described_class)
    end
  end

  describe '#inserted_count' do
    it 'is taken from results document' do
      expect(subject.inserted_count).to eql(2)
    end
  end

  describe '#inserted_ids' do
    it 'is taken from results document' do
      expect(subject.inserted_ids).to eql([1, 2])
    end
  end

  describe '#deleted_count' do
    let(:results_document) do
      {'n_removed' => 2, 'n' => 3}
    end

    it 'is taken from results document' do
      expect(subject.deleted_count).to eql(2)
    end
  end

  describe '#matched_count' do
    let(:results_document) do
      {'n_modified' => 1, 'n_matched' => 2, 'n' => 3}
    end

    it 'is taken from results document' do
      expect(subject.matched_count).to eql(2)
    end
  end

  describe '#modified_count' do
    let(:results_document) do
      {'n_modified' => 1, 'n_matched' => 2, 'n' => 3}
    end

    it 'is taken from results document' do
      expect(subject.modified_count).to eql(1)
    end
  end

  describe '#upserted_count' do
    let(:results_document) do
      {'n_upserted' => 2, 'n' => 3, 'upserted_ids' => [1, 2]}
    end

    it 'is taken from results document' do
      expect(subject.upserted_count).to eql(2)
    end
  end

  describe '#upserted_ids' do
    let(:results_document) do
      {'n_upserted' => 2, 'n' => 3, 'upserted_ids' => [1, 2]}
    end

    it 'is taken from results document' do
      expect(subject.upserted_ids).to eql([1, 2])
    end
  end

  describe '#validate!' do
    context 'no errors' do
      it 'returns self' do
        expect(subject.validate!).to eql(subject)
      end
    end

    context 'with top level error' do
      let(:results_document) do
        {
          'writeErrors' => [
            {
              'ok' => 0,
              'errmsg' => 'not master',
              'code' => 10107,
              'codeName' => 'NotMaster',
            }
          ]
        }
      end

      it 'raises BulkWriteError' do
        expect do
          subject.validate!
        # BulkWriteErrors don't have any messages on them
        end.to raise_error(Mongo::Error::BulkWriteError, /not master/)
      end
    end

    context 'with write concern error' do
      let(:results_document) do
        {'n' => 1, 'writeConcernErrors' => {
          'errmsg' => 'Not enough data-bearing nodes',
          'code' => 100,
          'codeName' => 'CannotSatisfyWriteConcern',
        }}
      end

      it 'raises BulkWriteError' do
        expect do
          subject.validate!
        # BulkWriteErrors don't have any messages on them
        end.to raise_error(Mongo::Error::BulkWriteError, nil)
      end
    end
  end

  describe "#acknowledged?" do

    [true, false].each do |b|
      context "when acknowledged is passed as #{b}" do

        let(:result) { described_class.new(results_document, b) }

        it "acknowledged? is #{b}" do
          expect(result.acknowledged?).to be b
        end
      end
    end
  end
end
