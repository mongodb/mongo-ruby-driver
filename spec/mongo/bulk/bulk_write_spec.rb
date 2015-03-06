require 'spec_helper'

describe Mongo::BulkWrite do

  before do
    authorized_collection.find.delete_many
  end

  after do
    authorized_collection.find.delete_many
  end

  let(:bulk) do
    described_class.get(authorized_collection, operations, options)
  end

  describe '#get' do

    let(:operations) do
      [{ insert_one: { _id: 0 } }]
    end

    context 'When an ordered bulk write object is created' do

      let(:options) do
        { ordered: true }
      end

      it 'returns an OrderedBulkWrite object' do
        expect(bulk).to be_a(Mongo::BulkWrite::OrderedBulkWrite)
      end
    end

    context 'When an unordered bulk write object is created' do

      let(:options) do
        { ordered: false }
      end

      it 'returns an UnorderedBulkWrite object' do
        expect(bulk).to be_a(Mongo::BulkWrite::UnorderedBulkWrite)
      end
    end
  end

  describe 'Ordered bulk write' do

    let(:options) do
       { ordered: true }
    end
     
    it_behaves_like 'a bulk write object'
    
    context 'when the insert batch requires splitting' do
    
      context 'when insert_one operations exceed the max batch size' do
    
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
          expect(error).to be_a(Mongo::Error::BulkWriteError)
        end
    
        it 'halts execution after first error and reports correct index' do
          # TODO uncomment to check for indexes
          #expect(error.result['writeErrors'].first['index']).to eq(3000)
          error
          expect(authorized_collection.find.count).to eq(3000)
        end
      end
    
    #   context 'when the operations exceed the max bson size' do
    
    #     let(:error) do
    #       begin
    #         bulk.execute
    #       rescue => ex
    #         ex
    #       end
    #     end
    
    #     let(:operations) do
    #       [].tap do |ops|
    #         6.times do |i|
    #           ops << { insert_one: { _id: i, x: 'y'*4000000 } }
    #         end
    #         ops << { insert_one: { _id: 0 } }
    #         ops << { insert_one: { _id: 100 } }
    #       end
    #     end
    
    #     it 'raises an error' do
    #       expect(error).to be_a(Mongo::Error::BulkWriteError)
    #     end
    
    #     it 'splits messages into multiple messages' do
    #       error
    #       expect(authorized_collection.find.count).to eq(6)
    #     end
    #   end
      end
    end

    describe 'Unordered bulk write' do

      let(:options) do
         { ordered: false }
      end

      it_behaves_like 'a bulk write object'

      context 'when the insert batch requires splitting' do

        context 'when the operations exceed the max batch size' do

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

          it 'raises an error' do
            expect(error).to be_a(Mongo::Error::BulkWriteError)
          end

          it 'does not halt execution after first error' do
            # TODO uncomment
            #expect(error.result['writeErrors'].first['index']).to eq(3000)
            error
            expect(authorized_collection.find.count).to eq(3001)
          end
        end

    #   context 'when the operations exceed the max bson size' do

    #     let(:error) do
    #       begin
    #         bulk.execute
    #       rescue => ex
    #         ex
    #       end
    #     end

    #     let(:operations) do
    #       [].tap do |ops|
    #         15.times do |i|
    #           ops << { insert_one: { _id: i, x: 'y'*4000000 } }
    #         end
    #         ops << { insert_one: { _id: 0 } }
    #         ops << { insert_one: { _id: 100 } }
    #       end
    #     end

    #     it 'raises an error' do
    #       expect(error).to be_a(Mongo::Error::BulkWriteError)
    #     end

    #     it 'splits messages into multiple messages' do
    #       error
    #       expect(authorized_collection.find.count).to eq(16)
    #     end
    #   end
    end
  end
end
