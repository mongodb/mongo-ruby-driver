require 'spec_helper'

describe Mongo::Bulk::BulkWrite do

  context 'ordered' do

    let(:bulk) do
      described_class.new(authorized_collection, ordered: true)
    end

    it_behaves_like 'a bulk write object'

    context 'insert batch splitting' do

      after do
        authorized_collection.find.remove_many
      end

      context 'operations exceed max batch size' do

        before do
          3000.times do |i|
            bulk.insert(_id: i)
          end
          bulk.insert(_id: 0)
          bulk.insert(_id: 3001)
        end

        after do
          authorized_collection.find.remove_many
        end

        # @todo should raise exception

        it 'halts execution after first error' do
          bulk.execute
          expect(authorized_collection.find.count).to eq(3000)
        end
      end

      context 'operations exceed max bson size' do

        before do
          6.times do |i|
            bulk.insert(_id: i, x: 'y'*4000000)
          end
          bulk.insert(_id: 0)
          bulk.insert(_id: 100)
        end

        after do
          authorized_collection.find.remove_many
        end

        # @todo should raise exception

        it 'splits messages in multiple message' do
          bulk.execute
          expect(authorized_collection.find.count).to eq(6)
        end
      end
    end
  end

  context 'unordered' do

    let(:bulk) do
      described_class.new(authorized_collection, ordered: false)
    end

    it_behaves_like 'a bulk write object'

    context 'insert batch splitting' do

      after do
        authorized_collection.find.remove_many
      end

      context 'operations exceed max batch size' do

        before do
          3000.times do |i|
            bulk.insert(_id: i)
          end
          bulk.insert(_id: 0)
          bulk.insert(_id: 3001)
        end

        after do
          authorized_collection.find.remove_many
        end

        # @todo should raise exception

        it 'does not halt execution after first error' do
          bulk.execute
          expect(authorized_collection.find.count).to eq(3001)
        end
      end
    end

    context 'operations exceed max bson size' do

      before do
        15.times do |i|
          bulk.insert(_id: i, x: 'y'*4000000)
        end
        bulk.insert(_id: 0)
        bulk.insert(_id: 100)
      end

      after do
        authorized_collection.find.remove_many
      end

      # @todo should raise exception

      it 'splits messages in multiple message' do
        bulk.execute
        expect(authorized_collection.find.count).to eq(16)
      end
    end
  end
end
