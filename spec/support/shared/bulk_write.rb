shared_examples 'a bulk write object' do

  context '#insert' do

    context 'hash argument' do

      #context 'when there are $-prefixed keys' do
      #
      #  it 'raises an exception' do
      #    expect{ bulk.insert('$in' => 'valid') }.to raise_exception
      #  end
      #end

      context 'when the doc is valid' do

        it 'inserts the doc into the database' do
          bulk.insert({})
          #expect{ bulk.execute }.to_not raise_error
        end
      end
    end

    context 'when non-hash arguments are passed in' do

      it 'raises an exception' do
        expect{ bulk.insert('foo') }.to raise_exception

        expect{ bulk.insert([]) }.to raise_exception
      end
    end

    context 'when find has been specified' do

      it 'raises an exception' do
        expect{ bulk.find({}).insert({}) }.to raise_exception
      end
    end

    context 'when a document is inserted' do
      let(:doc) { { '_id' => 1 } }

      before do
        #collection.drop
        #bulk.insert(doc)
        #result = bulk.execute
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
        #bulk.insert(doc)
        #result = bulk.execute
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
        expect{ bulk.find() }.to raise_exception
      end
    end
  end

  context '#update' do
    let(:update_doc) { { :$set => { 'a' => 1 } } }

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
      let(:docs) { [{ :a => 1 }, { :a => 1 }] }
      let(:expected) do
        docs.each do |doc|
          doc['x'] = 1
        end
      end

      before do
        #collection.drop
        #collection.insert(docs)
        #bulk.find({}).update(:$set => { :x => 1 })
        #result = bulk.execute
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
        let(:update_doc) { { :a => 1 } }

        it 'raises an exception' do
          expect do
            bulk.find({}).update_one(update_doc)
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
        #bulk.find({}).update_one(:$set => { :x => 1 })
        #result = bulk.execute
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
      expect{ bulk.find({}).replace(:x => 1)}.to raise_exception
    end
  end

  context '#replace_one' do
    let(:replacement) { { :a => 3 } }

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

      context 'when there are some $-operator top-level keys' do
        let(:replacement) { { :$set => { :a => 3 } } }

        it 'raises an exception' do

          expect do
            bulk.find({}).replace_one(replacement)
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
        bulk.find({}).replace_one(replacement)
        #result = bulk.execute
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
        expect{ bulk.upsert }.to raise_exception
      end
    end

    context '#upsert.update' do
      let(:expected) do
        { 'a' => 2, 'x' => 2 }
      end

      context 'when #upsert.update is chained with other updates' do
        before do
          #collection.drop
          bulk.find(:a => 1).update(:$set => { :x => 1 })
          bulk.find(:a => 2).upsert.update(:$set => { :x => 2 })
          #result = bulk.execute
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
            bulk.find(:a => 1).update(:$set => { :x => 1 })
            bulk.find(:a => 2).upsert.update(:$set => { 'x' => 2 })
            #result = bulk.execute
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
          #bulk.find(:a => 1).upsert.update(:$set => { 'x' => 1 })
          #result = bulk.execute
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
          #bulk.find(:a => 1).upsert.update(:$set => { :x => big_string })
          #expect{ bulk.execute }.not_to raise_error
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
          bulk.find(:a => 1).update_one(:$set => { :x => 1 })
          bulk.find(:a => 2).upsert.update_one(:$set => { :x => 2 })
          #result = bulk.execute
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
          bulk.find(:a => 1).replace_one(:x => 1)
          bulk.find(:a => 2).upsert.replace_one(:x => 2)
          #result = bulk.execute
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
          bulk.find(:a => 1).upsert.replace_one(:x => 1)
          #result = bulk.execute
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
        expect{ bulk.remove }.to raise_exception
      end
    end

    context 'empty query selector' do
      before do
        #collection.drop
        #collection.insert([ { :a => 1 }, { :a => 1 }])
        bulk.find({}).remove
        #result = bulk.execute
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
        bulk.find(:a => 1).remove
        #result = bulk.execute
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
        expect{ bulk.remove_one }.to raise_exception
      end
    end

    context 'multiple matching documents' do
      before do
        #collection.drop
        #collection.insert([ { :a => 1 }, { :a => 1 }])
        bulk.find(:a => 1).remove_one
        #result = bulk.execute
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

  context 'mixed operations' do

  end

  context 'mixed operations, auth' do

  end

  context 'errors' do

  end

  context 'batch splitting' do
    #let(:large_doc) do
    #  { 'a' => "y"*(2*Mongo::Server::Description::MAX_MESSAGE_BYTES) }
    #end
    #let(:doc) do
    #  { 'a' => "y"*(Mongo::Server::Description::MAX_MESSAGE_BYTES) }
    #end
    context 'doc exceeds max BSON object size' do

      it 'raises an exception' do
        #expect{ bulk.insert(large_doc) }.to raise_exception
      end
    end

    context 'operation exceeds max message size' do
      before do
        #collection.drop
        #3.times do
        #  bulk.insert(doc)
        #end
      end

      it 'splits the operations into multiple message' do
        #expect{ bulk.insert(large_doc) }.not_to raise_exception
      end
    end
  end

  context 're-running a batch' do
    before do
      #collection.drop
      #bulk.insert(:a => 1)
      #bulk.execute
    end
    after do
      #collection.drop
    end

    it 'raises an exception' do
      #expect{ bulk.execute }.to raise_exception
    end
  end

  context 'empty batch' do

    it 'raises an exception' do
      expect{ bulk.execute }.to raise_exception
    end
  end

  context 'j write concern used with no journal' do
    let(:write_concern) do
      { :w => 1, :j => 1 }
    end
    allow(bulk).to receive(:execute) { response }

    before do
      bulk.insert(:a => 1)
    end

    context 'version < 2.4' do
      let(response) do
        # @todo: mock a response object using the doc below
        {
            "ok" => 1,
            "n" => 1,
            "writeConcernError" => [
                {
                    "code" => 2,
                    "errmsg" => "journaling not enabled on this server",
                    "index" => 0
                }
            ],
            "code" => 65,
            "errmsg" => "batch item errors occurred",
            "nInserted" => 1
        }
      end

      it 'raises an error' do
        expect{ bulk.execute(write_concern) }.to raise_exception
      end
    end

    context 'version >= 2.6' do
      let(response) do
        # @todo: mock a response object using the doc below
        {
            "ok" => 0,
            "n" => 0,
            "writeErrors" => [
                {
                    "code" => 2,
                    "errmsg" => "cannot use 'j' option when a host does not have journaling enabled", "index" => 0
                }
            ],
            "code" => 65,
            "errmsg" => "batch item errors occurred",
            "nInserted" => 0
        }
      end

      allow(bulk).to receive(:execute) { response }

      it 'raises an error' do
        expect{ bulk.execute(write_concern) }.to raise_exception
      end
    end
  end

  context 'w > 1 against standalone' do
    let(:write_concern) do
      { :w => 2 }
    end
    before do
      bulk.insert(:a => 1)
    end

    it 'raises an error' do
      expect{ bulk.execute(write_concern) }.to raise_exception
    end
  end

  context 'failover with mixed versions' do

  end
end
