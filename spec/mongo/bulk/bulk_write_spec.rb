require 'spec_helper'

describe Mongo::BulkWrite do

  context 'ordered' do

    before do
      authorized_collection.find.remove_many
    end

    let(:options) do
      { ordered: true }
    end

    let(:bulk) do
      described_class.new(operations, options, authorized_collection)
    end

    it_behaves_like 'a bulk write object'

    context 'insert batch splitting' do

      context 'operations exceed max batch size' do

        let(:error) do
          begin
            bulk.execute
          rescue => ex
            ex
          end
        end

        let(:operations) do
          [].tap do |ops|
            3000.times do |i|
              ops << { insert_one: { _id: i } }
            end
            ops << { insert_one: { _id: 0 } }
            ops << { insert_one: { _id: 3001 } }
          end
        end

        it 'raises a BulkWriteError' do
          expect(error).to be_a(Mongo::Error::BulkWriteFailure)
        end

        it 'halts execution after first error and reports correct index' do
          expect(error.result['writeErrors'].first['index']).to eq(3000)
          expect(authorized_collection.find.count).to eq(3000)
        end
      end

      context 'operations exceed max bson size' do

        let(:error) do
          begin
            bulk.execute
          rescue => ex
            ex
          end
        end

        let(:operations) do
          [].tap do |ops|
            6.times do |i|
              ops << { insert_one: { _id: i, x: 'y'*4000000 } }
            end
            ops << { insert_one: { _id: 0 } }
            ops << { insert_one: { _id: 100 } }
          end
        end

        it 'raises a BulkWriteError' do
          expect(error).to be_a(Mongo::Error::BulkWriteFailure)
        end

        it 'splits messages into multiple messages' do
          error
          expect(authorized_collection.find.count).to eq(6)
        end
      end
    end
  end

  context 'unordered' do

    before do
      authorized_collection.find.remove_many
    end

    let(:options) do
      { ordered: false }
    end

    let(:bulk) do
      described_class.new(operations, options, authorized_collection)
    end

    it_behaves_like 'a bulk write object'

    context 'insert batch splitting' do

      context 'operations exceed max batch size' do

        let(:error) do
          begin
            bulk.execute
          rescue => ex
            ex
          end
        end

        let(:operations) do
          [].tap do |ops|
            3000.times do |i|
              ops << { insert_one: { _id: i } }
            end
            ops << { insert_one: { _id: 0 } }
            ops << { insert_one: { _id: 3001 } }
          end
        end

        after do
          authorized_collection.find.remove_many
        end

        it 'raises a BulkWriteError' do
          expect(error).to be_a(Mongo::Error::BulkWriteFailure)
        end

        it 'does not halt execution after first error' do
          expect(error.result['writeErrors'].first['index']).to eq(3000)
          expect(authorized_collection.find.count).to eq(3001)
        end
      end
    end

    context 'operations exceed max bson size' do

      let(:error) do
        begin
          bulk.execute
        rescue => ex
          ex
        end
      end

      let(:operations) do
        [].tap do |ops|
          15.times do |i|
            ops << { insert_one: { _id: i, x: 'y'*4000000 } }
          end
          ops << { insert_one: { _id: 0 } }
          ops << { insert_one: { _id: 100 } }
        end
      end

      after do
        authorized_collection.find.remove_many
      end

      it 'raises a BulkWriteError' do
        expect(error).to be_a(Mongo::Error::BulkWriteFailure)
      end

      it 'splits messages into multiple messages' do
        error
        expect(authorized_collection.find.count).to eq(16)
      end
    end
  end
end
