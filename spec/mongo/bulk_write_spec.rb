require 'spec_helper'

describe Mongo::BulkWrite do

  before do
    authorized_collection.delete_many
  end

  after do
    authorized_collection.delete_many
  end

  describe '#execute' do

    shared_examples_for 'an executable bulk write' do

      context 'when providing a bad operation' do

        let(:requests) do
          [{ not_an_operation: { _id: 0 }}]
        end

        it 'raises an exception' do
          expect {
            bulk_write.execute
          }.to raise_error(Mongo::Error::InvalidBulkOperationType)
        end
      end

      context 'when the operations do not need to be split' do

        context 'when a write error occurs' do

          let(:requests) do
            [
              { insert_one: { _id: 0 }},
              { insert_one: { _id: 1 }},
              { insert_one: { _id: 0 }},
              { insert_one: { _id: 1 }}
            ]
          end

          let(:error) do
            begin
              bulk_write.execute
            rescue => e
              e
            end
          end

          it 'raises an exception' do
            expect {
              bulk_write.execute
            }.to raise_error(Mongo::Error::BulkWriteError)
          end

          it 'sets the document index on the error' do
            expect(error.result[Mongo::Error::WRITE_ERRORS].first['index']).to eq(2)
          end
        end

        context 'when provided a single insert one' do

          let(:requests) do
            [{ insert_one: { _id: 0 }}]
          end

          let(:result) do
            bulk_write.execute
          end

          it 'inserts the document' do
            expect(result.inserted_count).to eq(1)
            expect(authorized_collection.find(_id: 0).count).to eq(1)
          end
        end

        context 'when provided multiple insert ones' do

          let(:requests) do
            [
              { insert_one: { _id: 0 }},
              { insert_one: { _id: 1 }},
              { insert_one: { _id: 2 }}
            ]
          end

          let(:result) do
            bulk_write.execute
          end

          it 'inserts the documents' do
            expect(result.inserted_count).to eq(3)
            expect(authorized_collection.find(_id: { '$in'=> [ 0, 1, 2 ]}).count).to eq(3)
          end
        end

        context 'when provided a single delete one' do

          let(:requests) do
            [{ delete_one: { filter: { _id: 0 }}}]
          end

          let(:result) do
            bulk_write.execute
          end

          before do
            authorized_collection.insert_one({ _id: 0 })
          end

          it 'deletes the document' do
            expect(result.deleted_count).to eq(1)
            expect(authorized_collection.find(_id: 0).count).to eq(0)
          end
        end

        context 'when provided multiple delete ones' do

          let(:requests) do
            [
              { delete_one: { filter: { _id: 0 }}},
              { delete_one: { filter: { _id: 1 }}},
              { delete_one: { filter: { _id: 2 }}}
            ]
          end

          let(:result) do
            bulk_write.execute
          end

          before do
            authorized_collection.insert_many([
              { _id: 0 }, { _id: 1 }, { _id: 2 }
            ])
          end

          it 'deletes the documents' do
            expect(result.deleted_count).to eq(3)
            expect(authorized_collection.find(_id: { '$in'=> [ 0, 1, 2 ]}).count).to eq(0)
          end
        end

        context 'when provided a single delete many' do

          let(:requests) do
            [{ delete_many: { filter: { _id: 0 }}}]
          end

          let(:result) do
            bulk_write.execute
          end

          before do
            authorized_collection.insert_one({ _id: 0 })
          end

          it 'deletes the documents' do
            expect(result.deleted_count).to eq(1)
            expect(authorized_collection.find(_id: 0).count).to eq(0)
          end
        end

        context 'when provided multiple delete many ops' do

          let(:requests) do
            [
              { delete_many: { filter: { _id: 0 }}},
              { delete_many: { filter: { _id: 1 }}},
              { delete_many: { filter: { _id: 2 }}}
            ]
          end

          let(:result) do
            bulk_write.execute
          end

          before do
            authorized_collection.insert_many([
              { _id: 0 }, { _id: 1 }, { _id: 2 }
            ])
          end

          it 'deletes the documents' do
            expect(result.deleted_count).to eq(3)
            expect(authorized_collection.find(_id: { '$in'=> [ 0, 1, 2 ]}).count).to eq(0)
          end
        end

        context 'when providing a single replace one' do

          let(:requests) do
            [{ replace_one: { filter: { _id: 0 }, replacement: { name: 'test' }}}]
          end

          let(:result) do
            bulk_write.execute
          end

          before do
            authorized_collection.insert_one({ _id: 0 })
          end

          it 'replaces the document' do
            expect(result.modified_count).to eq(1)
            expect(authorized_collection.find(_id: 0).first[:name]).to eq('test')
          end
        end

        context 'when providing a single update one' do

          let(:requests) do
            [{ update_one: { filter: { _id: 0 }, update: { "$set" => { name: 'test' }}}}]
          end

          let(:result) do
            bulk_write.execute
          end

          before do
            authorized_collection.insert_one({ _id: 0 })
          end

          it 'updates the document' do
            expect(result.modified_count).to eq(1)
            expect(authorized_collection.find(_id: 0).first[:name]).to eq('test')
          end
        end

        context 'when providing a single update many' do

          let(:requests) do
            [{ update_many: { filter: { _id: 0 }, update: { "$set" => { name: 'test' }}}}]
          end

          let(:result) do
            bulk_write.execute
          end

          before do
            authorized_collection.insert_one({ _id: 0 })
          end

          it 'updates the documents' do
            expect(result.modified_count).to eq(1)
            expect(authorized_collection.find(_id: 0).first[:name]).to eq('test')
          end
        end
      end

      context 'when the operations need to be split' do

        context 'when a write error occurs' do

          let(:requests) do
            1001.times.map do |i|
              { insert_one: { _id: i }}
            end
          end

          let(:error) do
            begin
              bulk_write.execute
            rescue => e
              e
            end
          end

          it 'raises an exception' do
            expect {
              requests.push({ insert_one: { _id: 5 }})
              bulk_write.execute
            }.to raise_error(Mongo::Error::BulkWriteError)
          end

          it 'sets the document index on the error' do
            requests.push({ insert_one: { _id: 5 }})
            expect(error.result[Mongo::Error::WRITE_ERRORS].first['index']).to eq(1001)
          end
        end

        context 'when no write errors occur' do

          let(:requests) do
            1001.times.map do |i|
              { insert_one: { _id: i }}
            end
          end

          let(:result) do
            bulk_write.execute
          end

          it 'inserts the documents' do
            expect(result.inserted_count).to eq(1001)
          end
        end
      end

      context 'when an operation exceeds the max bson size' do

        let(:requests) do
          5.times.map do |i|
            { insert_one: { _id: i, x: 'y' * 4000000 }}
          end
        end

        let(:result) do
          bulk_write.execute
        end

        it 'inserts the documents' do
          expect(result.inserted_count).to eq(5)
        end
      end
    end

    context 'when the bulk write is unordered' do

      let(:bulk_write) do
        described_class.new(authorized_collection, requests, ordered: false)
      end

      it_behaves_like 'an executable bulk write'
    end

    context 'when the bulk write is ordered' do

      let(:bulk_write) do
        described_class.new(authorized_collection, requests, ordered: true)
      end

      it_behaves_like 'an executable bulk write'
    end
  end

  describe '#initialize' do

    let(:requests) do
      [{ insert_one: { _id: 0 }}]
    end

    shared_examples_for 'a bulk write initializer' do

      it 'sets the collection' do
        expect(bulk_write.collection).to eq(authorized_collection)
      end

      it 'sets the requests' do
        expect(bulk_write.requests).to eq(requests)
      end
    end

    context 'when no options are provided' do

      let(:bulk_write) do
        described_class.new(authorized_collection, requests)
      end

      it 'sets empty options' do
        expect(bulk_write.options).to be_empty
      end

      it_behaves_like 'a bulk write initializer'
    end

    context 'when options are provided' do

      let(:bulk_write) do
        described_class.new(authorized_collection, requests, ordered: true)
      end

      it 'sets the options' do
        expect(bulk_write.options).to eq(ordered: true)
      end
    end

    context 'when nil options are provided' do

      let(:bulk_write) do
        described_class.new(authorized_collection, requests, nil)
      end

      it 'sets empty options' do
        expect(bulk_write.options).to be_empty
      end
    end
  end

  describe '#ordered?' do

    context 'when no option provided' do

      let(:bulk_write) do
        described_class.new(authorized_collection, [])
      end

      it 'returns true' do
        expect(bulk_write).to be_ordered
      end
    end

    context 'when the option is provided' do

      context 'when the option is true' do

        let(:bulk_write) do
          described_class.new(authorized_collection, [], ordered: true)
        end

        it 'returns true' do
          expect(bulk_write).to be_ordered
        end
      end

      context 'when the option is false' do

        let(:bulk_write) do
          described_class.new(authorized_collection, [], ordered: false)
        end

        it 'returns false' do
          expect(bulk_write).to_not be_ordered
        end
      end
    end
  end
end
