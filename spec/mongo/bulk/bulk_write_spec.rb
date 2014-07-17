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

    context 'hash argument' do

      #context 'when there are $-prefixed keys' do
#
      #  it 'raises an exception' do
      #    expect{ ordered_batch.insert('$in' => 'valid') }.to raise_exception
      #    expect{ unordered_batch.insert('$in' => 'valid') }.to raise_exception
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

      before do
        #collection.drop
        #ordered_batch.insert(doc)
        #result = ordered_batch.execute
      end

      after do
        #collection.drop
      end

      it 'returns nInserted of 1' do
        #expect(result['nInserted']).to eq(1)
      end

      it 'only inserts that document' do
        #expect(collection.find.to_a).to eq([ doc ])
      end
    end

    context '_id not in doc' do
      let(:doc) { {} }

      before do
        #collection.drop
        #ordered_batch.insert(doc)
        #result = ordered_batch.execute
      end

      after do
        #collection.drop
      end

      it 'inserts the doc into the database' do
        #expect(result['nInserted']).to eq(1)
      end

      it 'generates the _id client-side' do
        #doc = collection.find_one
        #pid = bytes 7 and 8 (counting from zero) of _id, as big-endian unsigned short
        #expect(pid).to eq(my pid)
      end
    end
  end

  context '#find' do

    context 'arguments' do

      it 'raises an exception if no args are provided' do
        expect{ ordered_batch.find() }.to raise_exception
      end
    end
  end

  context '#update' do
    let(:update_doc) { { :$set => { 'a' => 1 } } }

    context 'when find is not first specified' do

      it 'raises an exception' do
        expect{ ordered_batch.update(update) }.to raise_exception
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

      context 'when a non-hash argument is passed in' do

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
      let(:docs) { [{ :a => 1 }, { :a => 1 }] }
      let(:expected) do
        docs.each do |doc|
          doc['x'] = 1
        end
      end

      before do
        #collection.drop
        #collection.insert(docs)
        #ordered_batch.find({}).update(:$set => { :x => 1 })
        #result = ordered_batch.execute
      end

      after do
        #collection.drop
      end

      it 'applies the update to all matching documents' do
        #expect(collection.find(:x => 1).count).to eq(2)
      end

      it 'reports nMatched correctly' do
        #expect(result['nMatched']).to eq(2)
      end

      it 'only applies the update to the matching documents' do
        #expect(collection.find.to_a).to eq(expected)
      end
    end
  end

  context '#update_one' do
    let(:update_doc) { { :$set => { 'a' => 1 } } }

    context 'when find is not first specified' do

      it 'raises an exception' do
        expect{ ordered_batch.update(update) }.to raise_exception
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
        docs.tap do |d|
          d[0] = d[0].merge('x' => 1)
        end
      end

      before do
        #collection.drop
        #collection.insert(docs)
        #ordered_batch.find({}).update_one(:$set => { :x => 1 })
        #result = ordered_batch.execute
      end

      after do
        #collection.drop
      end

      it 'applies the update to only one matching document' do
        #expect(collection.find(:x => 1).count).to eq(1)
      end

      it 'reports nMatched correctly' do
        #expect(result['nMatched']).to eq(1)
      end

      it 'only applies the update to one matching document' do
        #expect(collection.find.to_a).to eq(expected)
      end
    end
  end

  context '#replace' do

    it 'does not exist' do
      expect{ ordered_batch.find({}).replace(:x => 1)}.to raise_exception
    end
  end

  context '#replace_one' do
    let(:replacement) { { :a => 3 } }

    context 'when find is not first specified' do

      it 'raises an exception' do
        expect{ ordered_batch.replace_one(replacement) }.to raise_exception
      end
    end

    context 'arguments' do

      context 'when a valid replacement doc is provided' do

        it 'does not raise an exception' do
          expect do
            ordered_batch.find({}).replace_one(replacement)
          end.not_to raise_exception
        end
      end

      context 'when an non-hash argument is passed in' do

        it 'raises an exception' do
          expect do
            ordered_batch.find({}).replace_one([])
          end.to raise_exception
        end
      end

      context 'when there are some $-operator top-level keys' do
        let(:replacement) { { :$set => { :a => 3 } } }

        it 'raises an exception' do

          expect do
            ordered_batch.find({}).replace_one(replacement)
          end.to raise_exception
        end
      end
    end

    context 'single replace' do
      let(:replacement) { { :a => 3 } }
      let(:docs) { [{ :a => 1 }, { :a => 1 }] }
      let(:expected) do
        docs.tap do |d|
          d[0] = { 'a' => 3 }
        end
      end

      before do
        #collection.drop
        #collection.insert(docs)
        ordered_batch.find({}).replace_one(replacement)
        #result = ordered_batch.execute
      end

      after do
        #collection.drop
      end

      it 'applies the replacement to only one matching document' do
        #expect(collection.find(replacement).count).to eq(1)
      end

      it 'reports nMatched correctly' do
        # @todo: nModified is NULL or omitted if legacy server
        #expect(result['nMatched']).to eq(1)
      end

      it 'only applies the replacement to one matching document' do
        #expect(collection.find.to_a).to eq(expected)
        # @todo: or do collection.distinct('a') == [1,3]
      end
    end
  end

  context '#upsert' do

    context 'when find is not first specified' do

      it 'raises an exception' do
        expect{ ordered_batch.upsert }.to raise_exception
      end
    end

    context '#upsert.update' do
      let(:expected) do
        { 'a' => 2, 'x' => 2 }
      end

      context 'when #upsert.update is chained with other updates' do
        before do
          #collection.drop
          ordered_batch.find(:a => 1).update(:$set => { :x => 1 })
          ordered_batch.find(:a => 2).upsert.update(:$set => { :x => 2 })
          #result = ordered_batch.execute
        end

        after do
          #collection.drop
        end

        it 'reports nModified correctly' do
          #@todo: nModified is NULL or omitted if legacy server
          #expect(result['nModified']).to eq(0)
        end

        it 'reports nUpserted correctly' do
          #expect(result['nUpserted']).to eq(1)
        end

        it 'only results in one single upserted doc' do
          #expect(collection.find.to_a).to eq(expected)
        end

        context 'when the bulk ops are repeated' do

          before do
            #collection.drop
            ordered_batch.find(:a => 1).update(:$set => { :x => 1 })
            ordered_batch.find(:a => 2).upsert.update(:$set => { 'x' => 2 })
            #result = ordered_batch.execute
          end

          after do
            #collection.drop
          end

          it 'reports nMatched correctly' do
            #expect(result['nMatched']).to eq(1)
          end

          it 'reports nUpserted correctly' do
            #expect(result['nUpserted']).to eq(0)
          end
        end
      end

      context 'when the selector matches multiple documents' do
        let(:docs) { [{ :a => 1 }, { :a => 1 }] }
        let(:expected) do
          docs.each do |d|
            d.merge!('x' => 1)
          end
        end

        before do
          #collection.drop
          #collection.insert(docs)
          #ordered_batch.find(:a => 1).upsert.update(:$set => { 'x' => 1 })
          #result = ordered_batch.execute
        end

        after do
          #collection.drop
        end

        it 'reports nModified correctly' do
          #expect(result['nModified']).to eq(2)
        end

        it 'reports nMatched correctly' do
          #expect(result['nMatched']).to eq(2)
        end

        it 'applies the update to all matching documents' do
          #expect(collection.find.to_a).to eq(expected)
        end
      end

      context 'when the document to upsert is 16MB' do
        let(:max_bson_size) { 4 * 1024 * 1024 } # @todo: minus 30 bytes
        let(:big_string) { 'a' * max_bson_size }

        it 'succesfully upserts the doc' do
          #ordered_batch.find(:a => 1).upsert.update(:$set => { :x => big_string })
          #expect{ ordered_batch.execute }.not_to raise_error
        end
      end
    end

    context '#upsert.update_one' do
      let(:expected) do
        { 'a' => 2, 'x' => 2 }
      end

      context 'when upsert.update_one is chained with other update_one ops' do

        before do
          #collection.drop
          ordered_batch.find(:a => 1).update_one(:$set => { :x => 1 })
          ordered_batch.find(:a => 2).upsert.update_one(:$set => { :x => 2 })
          #result = ordered_batch.execute
        end

        after do
          #collection.drop
        end

        it 'reports nModified correctly' do
          # @todo: nModified is NULL or omitted if legacy server
          #expect(result['nModified']).to eq(0)
        end

        it 'reports nUpserted correctly' do
          #expect(result['nUpserted']).to eq(1)
        end

        it 'reports nMatched correctly' do
          #expect(result['nMatched']).to eq(0)
        end

        it 'applies the correct writes' do
          #expect(collection.find.to_a).to eq(expected)
        end
      end
    end

    context '#upsert.replace_one' do

      context 'when upsert.replace_one is chained with other replace_one ops' do
        let(:expected) do
          { 'x' => 2 }
        end
        before do
          #collection.drop
          ordered_batch.find(:a => 1).replace_one(:x => 1)
          ordered_batch.find(:a => 2).upsert.replace_one(:x => 2)
          #result = ordered_batch.execute
        end

        after do
          #collection.drop
        end

        it 'reports nModified correctly' do
          # @todo: nModified is NULL or omitted if legacy server
          #expect(result['nModified']).to eq(0)
        end

        it 'reports nUpserted correctly' do
          #expect(result['nUpserted']).to eq(1)
        end

        it 'reports nMatched correctly' do
          #expect(result['nMatched']).to eq(0)
        end

        it 'applies the correct writes' do
          #expect(collection.find.to_a).to eq(expected)
        end
      end

      context 'one single document replacement' do
        let(:expected) do
          { 'a' => 1, 'x' => 1 }
        end

        before do
          #collection.drop
          #collection.insert([{ :a => 1 }, { :a => 2 }])
          ordered_batch.find(:a => 1).upsert.replace_one(:x => 1)
          #result = ordered_batch.execute
        end

        after do
          #collection.drop
        end

        it 'reports nUpserted correctly' do
          #expect(result['nUpserted']).to eq(0)
        end

        it 'reports nMatched correctly' do
          #expect(result['nMatched']).to eq(1)
        end

        it 'reports nModified correctly' do
          #expect(result['nMatched']).to eq(1)
        end

        it 'applies the correct writes' do
          #expect(collection.find.to_a).to eq(expected)
        end
      end
    end
  end

  context '#remove' do

    context 'when find is not first specified' do

      it 'raises an exception' do
        expect{ ordered_batch.remove }.to raise_exception
      end
    end

    context 'empty query selector' do
      before do
        #collection.drop
        #collection.insert([ { :a => 1 }, { :a => 1 }])
        ordered_batch.find({}).remove
        #result = ordered_batch.execute
      end

      after do
        #collection.drop
      end

      it 'reports nRemoved correctly' do
        #expect(result['nRemoved']).to eq(2)
      end

      it 'removes all documents' do
        #expect(collection.find.to_a).to eq([])
      end
    end

    context 'non-empty query selector' do
      before do
        #collection.drop
        #collection.insert([ { :a => 1 }, { :a => 2 }])
        ordered_batch.find(:a => 1).remove
        #result = ordered_batch.execute
      end

      after do
        #collection.drop
      end

      it 'reports nRemoved correctly' do
        #expect(result['nRemoved']).to eq(1)
      end

      it 'removes only matching documents' do
        #expect(collection.find.to_a).to eq({ :a => 2 })
      end
    end
  end

  context '#remove_one' do

    context 'when find is not first specified' do

      it 'raises an exception' do
        expect{ ordered_batch.remove_one }.to raise_exception
      end
    end

    context 'multiple matching documents' do
      before do
        #collection.drop
        #collection.insert([ { :a => 1 }, { :a => 1 }])
        ordered_batch.find(:a => 1).remove_one
        #result = ordered_batch.execute
      end

      after do
        #collection.drop
      end

      it 'reports nRemoved correctly' do
        #expect(result['nRemoved']).to eq(1)
      end

      it 'removes only matching documents' do
        #expect(collection.count).to eq(1)
      end
    end
  end

  context 'mixed operations, unordered' do

  end

  context 'mixed operations, ordered' do

  end

  context 'mixed operations, auth' do

  end

  context 'errors' do

    context 'unordered' do

    end

    context 'ordered' do

    end
  end

  context 'batch splitting' do
    let(:large_doc) do
      { 'a' => "y"*(2*Mongo::Server::Description::MAX_MESSAGE_BYTES) }
    end
    let(:doc) do
      { 'a' => "y"*(Mongo::Server::Description::MAX_MESSAGE_BYTES) }
    end
    context 'doc exceeds max BSON object size' do

      it 'raises an exception' do
        #expect{ ordered_batch.insert(large_doc) }.to raise_exception
      end
    end

    context 'operation exceeds max message size' do
      before do
        #collection.drop
        3.times do
          ordered_batch.insert(doc)
        end
      end

      it 'splits the operations into multiple message' do
        #expect{ ordered_batch.insert(large_doc) }.not_to raise_exception
      end
    end
  end

  context 're-running a batch' do

  end

  context 'empty batch' do

  end

  context 'no journal' do

  end

  context 'w > 1 against standalone' do

  end

  context 'wtimeout and duplicate key error' do

  end

  context 'w = 0' do

  end

  context 'failover with mixed versions' do

  end
end