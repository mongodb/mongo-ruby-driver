shared_examples 'a bulk write object' do

  context '#insert' do

    context 'hash argument' do

      context 'when there are $-prefixed keys' do
      
        pending 'raises an exception'
        #  expect do
        #    bulk.insert('$in' => 'valid')
        #  end.to raise_exception
        #end
      end

      context 'when the doc is valid' do

        it 'inserts the doc into the database' do
          bulk.insert({})
          expect do
            bulk.execute
          end.to_not raise_error
        end
      end
    end

    context 'when non-hash arguments are provided' do

      it 'raises an exception' do
        expect do
          bulk.insert('foo')
        end.to raise_exception

        expect do
          bulk.insert([])
        end.to raise_exception
      end
    end

    context 'when find has been specified' do

      it 'raises an exception' do
        expect do
          bulk.find({}).insert({})
        end.to raise_exception
      end
    end

    context 'when a document is inserted' do

      let(:doc) do
        { name: 'test' }
      end

      after do
        authorized_collection.find.remove_many
      end

      it 'returns nInserted of 1' do
        bulk.insert(doc)
        expect(
          bulk.execute['nInserted']
        ).to eq(1)
      end

      it 'only inserts that document' do
        bulk.insert(doc).execute
        expect(authorized_collection.find.first['name']).to eq('test')
      end
    end

    context 'when multiple documents are inserted' do

      let(:documents) do
        [ { name: 'test' }, { name: 'testing' } ]
      end

      after do
        authorized_collection.find.remove_many
      end

      pending 'returns nInserted of 2'
      #  documents.each do |doc|
      #    bulk.insert(doc)
      #  end
      #  expect(
      #    bulk.execute['nInserted']
      #  ).to eq(2)
      #end

      it 'only inserts those documents' do
        documents.each do |doc|
          bulk.insert(doc)
        end
        bulk.execute
        expect(authorized_collection.find.count).to eq(2)
      end
    end

    context '_id not in doc' do
      let(:doc) { {} }

      before do
        authorized_collection.drop
        bulk.insert(doc)
      end

      after do
        authorized_collection.find.remove_many
      end

      it 'inserts the doc into the database' do
        expect(bulk.execute['nInserted']).to eq(1)
      end

      pending 'generates the _id client-side'
    #     #doc = authorized_collection.find_one
    #     #pid = bytes 7 and 8 (counting from zero) of _id, as big-endian unsigned short
    #     #expect(pid).to eq(my pid)
    #   end
     end
  end

  context '#find' do

    context 'arguments' do

      it 'raises an exception if no args are provided' do
        expect{ bulk.find() }.to raise_exception
      end
    end
  end

  context '#update' do

    let(:update_doc) do
      { :$set => { 'a' => 1 } }
    end

    context 'when find is not first specified' do

      it 'raises an exception' do
        expect{ bulk.update(update) }.to raise_exception
      end
    end

    context 'arguments' do

      context 'when a valid update doc is provided' do

        it 'does not raise an exception' do
          expect do
            bulk.find({}).update(update_doc)
          end.not_to raise_exception
        end
      end

      context 'when a non-hash argument is passed in' do

        it 'raises an exception' do
          expect do
            bulk.find({}).update([])
          end.to raise_exception
        end
      end

      context 'when not all top-level keys are $-operators' do
        let(:update_doc) { { :a => 1 } }

        it 'raises an exception' do
          expect do
            bulk.find({}).update(update_doc)
          end.to raise_exception
        end
      end
    end

    context 'multi updates' do

      let(:docs) do
        [ { 'a' => 1 }, { 'a' => 1 } ]
      end

      let(:expected) do
        [ { 'a' => 1, 'x' => 1 },
          { 'a' => 1, 'x' => 1  } ]
      end

      before do
        authorized_collection.insert_many(docs)
        bulk.find({}).update(:$set => { x: 1 })
      end

      after do
        authorized_collection.find.remove_many
      end

      it 'applies the update to all matching documents' do
        bulk.execute
        expect(authorized_collection.find(x: 1).count).to eq(2)
      end

      it 'reports nMatched correctly' do
        expect(bulk.execute['nMatched']).to eq(2)
      end

      it 'only applies the update to the matching documents' do
        bulk.execute
        expect(authorized_collection.find.projection(_id: 0).to_a).to eq(expected)
      end
    end
  end

  context '#update_one' do

    let(:update_doc) do
      { :$set => { 'a' => 1 } }
    end

    context 'when find is not first specified' do

      it 'raises an exception' do
        expect{ bulk.update(update) }.to raise_exception
      end
    end

    context 'arguments' do

      context 'when a valid update doc is provided' do

        it 'does not raise an exception' do
          expect do
            bulk.find({}).update_one(update_doc)
          end.not_to raise_exception
        end
      end

      context 'when an non-hash argument is passed in' do

        it 'raises an exception' do
          expect do
            bulk.find({}).update_one([])
          end.to raise_exception
        end
      end

      context 'when not all top-level keys are $-operators' do

        let(:update_doc) do
          { :a => 1 }
        end

        it 'raises an exception' do
          expect do
            bulk.find({}).update_one(update_doc)
          end.to raise_exception
        end
      end
    end

    context 'single update' do

      let(:docs) do
        [ { 'a' => 1 }, { 'a' => 2 } ]
      end

      let(:expected) do
        [ { 'a' => 1, 'x' => 1 } ]
      end

      before do
        authorized_collection.insert_many(docs)
        bulk.find(a: 1).update(:$set => { x: 1 })
      end

      after do
        authorized_collection.find.remove_many
      end

      it 'applies the update to only one matching document' do
        bulk.execute
        expect(authorized_collection.find(x: 1).count).to eq(1)
      end

      it 'reports nMatched correctly' do
        expect(bulk.execute['nMatched']).to eq(1)
      end

      it 'only applies the update to one matching document' do
        bulk.execute
        docs = authorized_collection.find(x: 1).projection(_id: 0).to_a
        expect(docs).to eq(expected)
      end
    end
  end

  context '#replace' do

    it 'does not exist' do
      expect do
        bulk.find({}).replace(:x => 1)
      end.to raise_exception
    end
  end

  context '#replace_one' do

    let(:replacement) do
      { a: 3 }
    end

    context 'when find is not first specified' do

      it 'raises an exception' do
        expect{ bulk.replace_one(replacement) }.to raise_exception
      end
    end

    context 'arguments' do

      context 'when a valid replacement doc is provided' do

        it 'does not raise an exception' do
          expect do
            bulk.find({}).replace_one(replacement)
          end.not_to raise_exception
        end
      end

      context 'when an non-hash argument is passed in' do

        it 'raises an exception' do
          expect do
            bulk.find({}).replace_one([])
          end.to raise_exception
        end
      end

      context 'when there are $-operator top-level keys' do

        let(:replacement) do
          { :$set => { a: 3 } }
        end

        it 'raises an exception' do

          expect do
            bulk.find({}).replace_one(replacement)
          end.to raise_exception
        end
      end
    end

    context 'single replace' do

      let(:replacement) do
        { :a => 3 }
      end

      let(:docs) do
        [ { a: 1 }, { a: 1 } ]
      end

      let(:expected) do
        [ { 'a' => 3 }, { 'a' => 1 } ]
      end

      before do
        authorized_collection.insert_many(docs)
        bulk.find(a: 1).replace_one(replacement)
      end

      after do
        authorized_collection.find.remove_many
      end

      it 'applies the replacement to only one matching document' do
        bulk.execute
        expect(authorized_collection.find(replacement).count).to eq(1)
      end

      it 'reports nMatched correctly' do
        expect(bulk.execute['nMatched']).to eq(1)
      end

      it 'only applies the replacement to one matching document' do
        bulk.execute
        expect(authorized_collection.find.projection(_id: 0).to_a).to eq(expected)
      end
    end
  end

  context '#upsert' do

    context 'when find is not first specified' do

      it 'raises an exception' do
        expect do
          bulk.upsert
        end.to raise_exception
      end
    end

    context '#upsert.update' do

      let(:expected) do
        [ { 'a' => 2, 'x' => 2 } ]
      end

      context 'when #upsert.update is chained with other updates' do

        before do
          bulk.find(a: 1).update(:$set => { x: 1 })
          bulk.find(a: 2).upsert.update(:$set => { x: 2 })
        end

        after do
          authorized_collection.find.remove_many
        end

        it 'reports nModified correctly', if: write_command_enabled?  do
          expect(bulk.execute['nModified']).to eq(0)
        end

        it 'reports nModified as nil', unless: write_command_enabled?  do
          expect(bulk.execute['nModified']).to eq(nil)
        end

        it 'reports nUpserted correctly' do
          expect(bulk.execute['nUpserted']).to eq(1)
        end

        it 'only results in one single upserted doc' do
          bulk.execute
          expect(authorized_collection.find.projection(_id: 0).to_a).to eq(expected)
        end
      end

      context 'when the selector matches multiple documents' do

        let(:docs) do
          [ { a: 1 }, { a: 1 } ]
        end

        let(:expected) do
          [ { 'a' => 1, 'x' => 1}, { 'a' => 1, 'x' => 1} ]
        end

        before do
          authorized_collection.insert_many(docs)
          bulk.find(a: 1).upsert.update(:$set => { x: 1 })
        end

        after do
          authorized_collection.find.remove_many
        end

        it 'reports nModified as multiple', if: write_command_enabled?  do
          expect(bulk.execute['nModified']).to eq(2)
        end

        it 'reports nModified as nil', unless: write_command_enabled?  do
          expect(bulk.execute['nModified']).to eq(nil)
        end

        it 'reports nMatched as multiple' do
          expect(bulk.execute['nMatched']).to eq(2)
        end

        it 'applies the update to all matching documents' do
          bulk.execute
          expect(authorized_collection.find.projection(_id: 0).to_a).to eq(expected)
        end
      end

      context 'when the document to upsert is 16MB' do
        let(:max_bson_size) { 4 * 1024 * 1024 } # @todo: minus 30 bytes
        let(:big_string) { 'a' * max_bson_size }

        it 'succesfully upserts the doc' do
          # @todo
          #bulk.find(:a => 1).upsert.update(:$set => { :x => big_string })
          #expect{ bulk.execute }.not_to raise_error
        end
      end
    end

    context '#upsert.update_one' do

      let(:expected) do
        [ { 'a' => 2, 'x' => 2 } ]
      end

      context 'when upsert.update_one is chained with other update_one ops' do

        before do
          bulk.find(:a => 1).update_one(:$set => { :x => 1 })
          bulk.find(:a => 2).upsert.update_one(:$set => { :x => 2 })
        end

        after do
          authorized_collection.find.remove_many
        end

        it 'reports nModified correctly', if: write_command_enabled?  do
          expect(bulk.execute['nModified']).to eq(0)
        end

        it 'reports nModified as nil', unless: write_command_enabled?  do
          expect(bulk.execute['nModified']).to eq(nil)
        end

        it 'reports nUpserted correctly' do
          expect(bulk.execute['nUpserted']).to eq(1)
        end

        it 'reports nMatched correctly' do
          expect(bulk.execute['nMatched']).to eq(0)
        end

        it 'applies the correct writes' do
          bulk.execute
          expect(authorized_collection.find.projection(_id: 0).to_a).to eq(expected)
        end
      end
    end

    context '#upsert.replace_one' do

      context 'when upsert.replace_one is chained with other replace_one ops' do

        let(:expected) do
          [ { 'x' => 2 } ]
        end

        before do
          bulk.find(:a => 1).replace_one(:x => 1)
          bulk.find(:a => 2).upsert.replace_one(:x => 2)
        end

        after do
          authorized_collection.find.remove_many
        end

        it 'reports nModified as 0', if: write_command_enabled?  do
          expect(bulk.execute['nModified']).to eq(0)
        end

        it 'reports nModified as nil', unless: write_command_enabled?  do
          expect(bulk.execute['nModified']).to eq(nil)
        end

        it 'reports nUpserted correctly' do
          expect(bulk.execute['nUpserted']).to eq(1)
        end

        it 'reports nMatched correctly' do
          expect(bulk.execute['nMatched']).to eq(0)
        end

        it 'applies the correct writes' do
          bulk.execute
          expect(authorized_collection.find.projection(_id: 0).to_a).to eq(expected)
        end
      end

      context 'one single document replacement' do

        let(:docs) do
          [ { 'a' => 1 }, { 'a' => 2 } ]
        end

        let(:expected) do
          [ { 'x' => 1 }, {'a' => 2 } ]
        end

        before do
          authorized_collection.insert_many(docs)
          bulk.find(:a => 1).upsert.replace_one(:x => 1)
        end

        after do
          authorized_collection.find.remove_many
        end

        it 'reports nUpserted correctly' do
          expect(bulk.execute['nUpserted']).to eq(0)
        end

        it 'reports nMatched correctly' do
          expect(bulk.execute['nMatched']).to eq(1)
        end

        it 'reports nModified correctly' do
          expect(bulk.execute['nMatched']).to eq(1)
        end

        it 'applies the correct writes' do
          bulk.execute
          expect(authorized_collection.find.projection(_id: 0).to_a).to eq(expected)
        end
      end
    end
  end

  context '#remove' do

    context 'when find is not first specified' do

      it 'raises an exception' do
        expect do
          bulk.remove
        end.to raise_exception
      end
    end

    context 'empty query selector' do

      let(:docs) do
        [ { a: 1 }, { a: 2 } ]
      end

      before do
        authorized_collection.insert_many(docs)
        bulk.find({}).remove
      end

      after do
        authorized_collection.find.remove_many
      end

      it 'reports nRemoved correctly' do
        expect(bulk.execute['nRemoved']).to eq(2)
      end

      it 'removes all documents' do
        bulk.execute
        expect(authorized_collection.find.to_a).to be_empty
      end
    end

    context 'non-empty query selector' do

      let(:docs) do
        [ { a: 1 }, { a: 2 } ]
      end

      let(:expected) do
        [ { 'a' => 2 } ]
      end

      before do
        authorized_collection.insert_many(docs)
        bulk.find(:a => 1).remove
      end

      after do
        authorized_collection.find.remove_many
      end

      it 'reports nRemoved correctly' do
        expect(bulk.execute['nRemoved']).to eq(1)
      end

      it 'removes only matching documents' do
        bulk.execute
        expect(authorized_collection.find.projection(_id: 0).to_a).to eq(expected)
      end
    end
  end

  context '#remove_one' do

    context 'when find is not first specified' do

      it 'raises an exception' do
        expect do
          bulk.remove_one
        end.to raise_exception
      end
    end

    context 'multiple matching documents' do

      let(:docs) do
        [ { a: 1 }, { a: 1 } ]
      end

       let(:expected) do
        [ { 'a' => 1 } ]
      end

      before do
        authorized_collection.insert_many(docs)
        bulk.find(:a => 1).remove_one
      end

      after do
        authorized_collection.find.remove_many
      end

      it 'reports nRemoved correctly' do
        expect(bulk.execute['nRemoved']).to eq(1)
      end

      it 'removes only matching documents' do
        bulk.execute
        expect(authorized_collection.find.projection(_id: 0).to_a).to eq(expected)
      end
    end
  end

  context 're-running a batch' do

    before do
      bulk.insert(:a => 1)
      bulk.execute
    end

    after do
      authorized_collection.find.remove_many
    end

    it 'raises an exception' do
      expect do
        bulk.execute
      end.to raise_exception
    end
  end

  context 'empty batch' do

    it 'raises an exception' do
      expect do
        bulk.execute
      end.to raise_exception
    end
  end

  context 'when batches exceed max batch size' do

    context 'delete batch splitting' do

      before do
        3000.times do |i|
          authorized_collection.insert_one(_id: i)
        end
      end

      after do
        authorized_collection.find.remove_many
      end

      context 'operations exceed max batch size' do

        before do
          3000.times do |i|
            bulk.find(_id: i).remove_one
          end
        end

        it 'completes all operations' do
          bulk.execute
          expect(authorized_collection.find.count).to eq(0)
        end
      end
    end

    context 'update batch splitting' do

      before do
        3000.times do |i|
          authorized_collection.insert_one(x: i)
        end
      end

      after do
        authorized_collection.find.remove_many
      end

      context 'operations exceed max batch size' do

        before do
          3000.times do |i|
            bulk.find(x: i).update_one('$set' => { x: 6000-i })
          end
        end

        it 'completes all operations' do
          bulk.execute
          expect(authorized_collection.find(x: { '$gte' => 3000 }).count).to eq(3000)
        end
      end
    end
  end
end
