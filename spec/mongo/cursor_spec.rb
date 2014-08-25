require 'spec_helper'

describe Mongo::Cursor do

  describe '#each' do

    context 'when no options are provided to the view' do

      context 'when the initial query retieves all documents' do

      end

      context 'when the initial query does not retrieve all documents' do

      end
    end

    context 'when options are provided to the view' do

      context 'when a limit is provided' do

        context 'when no batch size is provided' do

          context 'when the limit is positive' do

          end

          context 'when the limit is negative' do

          end

          context 'when the limit is zero' do

          end
        end

        context 'when a batch size is provided' do

          context 'when the batch size is less than the limit' do

          end

          context 'when the batch size is more than the limit' do

          end

          context 'when the batch size is the same as the limit' do

          end
        end
      end
    end
  end

  describe '#inspect' do

    let(:view) do
      Mongo::CollectionView.new(authorized_client[TEST_COLL])
    end

    let(:query_spec) do
      { :selector => {}, :opts => {}, :db_name => TEST_DB, :coll_name => TEST_COLL }
    end

    let(:reply) do
      Mongo::Operation::Read::Query.new(query_spec)
    end

    let(:cursor) do
      described_class.new(view, reply, authorized_primary)
    end

    it 'returns a string' do
      expect(cursor.inspect).to be_a(String)
    end

    it 'returns a string containing the collection view inspect string' do
      expect(cursor.inspect).to match(/.*#{view.inspect}.*/)
    end
  end
end
