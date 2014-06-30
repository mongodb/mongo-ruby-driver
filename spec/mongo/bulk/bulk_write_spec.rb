require 'spec_helper'

describe Mongo::Bulk::BulkWrite do

  let(:write_concern) { Mongo::WriteConcern::Mode.get(:w => 1) }
  let(:database) { Mongo::Database.new(double('client'), :test) }
  let(:collection) do
    Mongo::Collection.new(database, 'users').tap do |c|
      allow(c).to receive(:write_concern) { write_concern }
    end
  end
  let(:ordered_batch) { described_class.new(collection, :ordered => true) }
  let(:unordered_batch) { described_class.new(collection, :ordered => false) }

  context '#insert' do
    #before(:each) { collection.drop }

    context 'hash argument' do

      #context 'when there are $-prefixed keys' do

      #  it 'raises an exception' do
      #    expect{ ordered_batch.insert({ '$in' => 'valid' }) }.to raise_exception
      #    expect{ unordered_batch.insert({ '$in' => 'valid' }) }.to raise_exception
      #  end
      #end

      context 'when the doc is valid' do

        it 'inserts the doc into the database' do
          ordered_batch.insert({})
          #expect{ ordered_batch.execute }.to_not raise_error
        end
      end
    end

    context 'when non-hash arguments are passed in' do

      it 'raises an exception' do
        expect{ ordered_batch.insert('foo') }.to raise_exception

        expect{ ordered_batch.insert([]) }.to raise_exception
      end
    end

    context 'when find has been specified' do

      it 'raises an exception' do
        expect{ ordered_batch.find({}).insert({}) }.to raise_exception
      end
    end

    context 'when a document is inserted' do
      let(:doc) { { '_id' => 1 } }

      before(:all) do
        #ordered_batch.insert(doc)
      end

      it 'returns nInserted of 1' do
        #expect(ordered_batch.execute['nInserted']).to eq(1)
      end

      it 'only inserts that document' do
        #expect(collection.find_one).to eq(doc)
      end
    end

    context '_id not in doc' do
      let(:doc) { {} }

      before(:all) do
        #ordered_batch.insert(doc)
      end

      it 'inserts the doc into the database' do
        #expect(batch.execute['nInserted']).to eq(1)
      end

      it 'generates the _id client-side' do
        #doc = collection.find_one
        #pid = bytes 7 and 8 (counting from zero) of _id, as big-endian unsigned short
       # expect(pid).to eq(my pid)
      end
    end
  end

  context 'find' do

    context 'arguments' do
      it 'raises an exception if no args are provided' do
        expect{ ordered_batch.find() }.to raise_exception
      end
    end
  end

  context '#update' do
    #before(:each) { collection.drop }
    let(:update_doc) { { :$set => { 'a' => 1 } } }

    context 'when find is not first specified' do

      it 'raises an exception' do
        expect{ ordered_batch.update(update)}.to raise_exception
      end
    end

    context 'arguments' do

      context 'when a valid update doc is provided' do
        it 'does not raise an exception' do
          expect do
            ordered_batch.find({}).update(update_doc)
          end.not_to raise_exception
        end
      end

      context 'when an non-hash argument is passed in' do
        it 'raises an exception' do
          expect do
            ordered_batch.find({}).update([])
          end.to raise_exception
        end
      end

      context 'when not all top-level keys are $-operators' do
        let(:update_doc) { { :a => 1 } }
        it 'raises an exception' do
          expect do
            ordered_batch.find({}).update(update_doc)
          end.to raise_exception
        end
      end
    end

    context 'multi updates' do
      let(:docs) { [{ 'a' => 1 }, { 'a' => 1 }] }
      let(:expected) do
        docs.each do |doc|
          doc['x'] = 1
        end
      end

      before do
        #collection.insert(docs)
        #ordered_batch.find({}).update({ :$set => { 'x' => 1 } })
        #result = ordered_batch.execute
      end

      it 'applies the update to all matching documents' do
        #expect(collection.find({ 'x' => 1 }).count).to eq(2)
      end

      it 'reports nMatched correctly' do
        #expect(result['nMatched']).to eq(2)
      end

      it 'only applies the update to the matching documents' do
        #expect(collection.find().to_a).to eq(expected)
      end
    end
  end

  context '#update_one' do
    #before(:each) { collection.drop }
    let(:update_doc) { { :$set => { 'a' => 1 } } }

    context 'when find is not first specified' do

      it 'raises an exception' do
        expect{ ordered_batch.update(update)}.to raise_exception
      end
    end

    context 'arguments' do

      context 'when a valid update doc is provided' do
        it 'does not raise an exception' do
          expect do
            ordered_batch.find({}).update_one(update_doc)
          end.not_to raise_exception
        end
      end

      context 'when an non-hash argument is passed in' do
        it 'raises an exception' do
          expect do
            ordered_batch.find({}).update_one([])
          end.to raise_exception
        end
      end

      context 'when not all top-level keys are $-operators' do
        let(:update_doc) { { :a => 1 } }
        it 'raises an exception' do
          expect do
            ordered_batch.find({}).update_one(update_doc)
          end.to raise_exception
        end
      end
    end

    context 'single update' do
      let(:docs) { [{ 'a' => 1 }, { 'a' => 1 }] }
      let(:expected) do
        docs.tap do |docs|
          doc[0] = doc[0].merge('x' => 1)
        end
      end

      before do
        #collection.insert(docs)
        #ordered_batch.find({}).update_one({ :$set => { 'x' => 1 } })
        #result = ordered_batch.execute
      end

      it 'applies the update to all matching documents' do
        #expect(collection.find({ 'x' => 1 }).count).to eq(1)
      end

      it 'reports nMatched correctly' do
        #expect(result['nMatched']).to eq(1)
      end

      it 'only applies the update to the matching documents' do
        #expect(collection.find().to_a).to eq(expected)
      end
    end
  end
end