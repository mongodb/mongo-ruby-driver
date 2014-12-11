shared_examples 'a bulk write object' do

  context 'insert_one' do

    context 'when a document is provided' do

      let(:operations) do
        [ { insert_one: { name: 'test' } } ]
      end
  
      it 'returns nInserted of 1' do
        expect(
          bulk.execute['nInserted']
        ).to eq(1)
      end

      it 'only inserts that document' do
        bulk.execute
        expect(authorized_collection.find.first['name']).to eq('test')
      end
    end

    context 'when non-hash doc is provided' do

      let(:operations) do
        [ { insert_one: [] } ]
      end

      it 'raises an InvalidDoc exception' do
        expect do
          bulk.execute
        end.to raise_error(Mongo::BulkWrite::InvalidDoc)
      end
    end
  end

  context 'delete_one' do

    let(:docs) do
      [ { a: 1 }, { a: 1 } ]
    end

     let(:expected) do
      [ { 'a' => 1 } ]
    end

    before do
      authorized_collection.insert_many(docs)
    end

    let(:operations) do
      [ { delete_one: { a: 1 } } ]
    end

    context 'when no selector is specified' do
      let(:operations) do
        [ { delete_one: nil } ]
      end

      it 'raises an exception' do
        expect do
          bulk.execute
        end.to raise_exception(Mongo::BulkWrite::InvalidDoc)
      end
    end

    context 'when multiple documents match delete selector' do

      it 'reports nRemoved correctly' do
        expect(bulk.execute['nRemoved']).to eq(1)
      end

      it 'deletes only matching documents' do
        bulk.execute
        expect(authorized_collection.find.projection(_id: 0).to_a).to eq(expected)
      end
    end
  end

  context 'delete_many' do

    let(:docs) do
      [ { a: 1 }, { a: 1 } ]
    end

    before do
      authorized_collection.insert_many(docs)
    end

    let(:operations) do
      [ { delete_many: { a: 1 } } ]
    end

    context 'when no selector is specified' do

      let(:operations) do
        [ { delete_many: nil } ]
      end

      it 'raises an exception' do
        expect do
          bulk.execute
        end.to raise_exception(Mongo::BulkWrite::InvalidDoc)
      end
    end

    context 'when a selector is specified' do

      context 'when multiple documents match delete selector' do

        it 'reports nRemoved correctly' do
          expect(bulk.execute['nRemoved']).to eq(2)
        end

        it 'deletes all matching documents' do
          bulk.execute
          expect(authorized_collection.find.to_a).to be_empty
        end
      end

      context 'when only one document matches delete selector' do

        let(:docs) do
          [ { a: 1 }, { a: 2 } ]
        end

        let(:expected) do
          [ { 'a' => 2 } ]
        end

        it 'reports nRemoved correctly' do
          expect(bulk.execute['nRemoved']).to eq(1)
        end

        it 'deletes all matching documents' do
          bulk.execute
          expect(authorized_collection.find.projection(_id: 0).to_a).to eq(expected)
        end
      end
    end
  end

  context 'replace_one' do

    let(:docs) do
      [ { a: 1 }, { a: 1 } ]
    end

    let(:expected) do
      [ { 'a' => 2 }, { 'a' => 1 } ]
    end

    before do
      authorized_collection.insert_many(docs)
    end

    let(:replacement) do
      { a: 2 }
    end

    let(:operations) do
      [ { replace_one: [ { a: 1 }, replacement ] } ]
    end

    context 'when a replace document is not specified' do

      let(:operations) do
        [ { replace_one: [ { a: 1 } ] } ]
      end

      it 'raises an exception' do
        expect do
          bulk.execute
        end.to raise_exception(ArgumentError)
      end
    end

    context 'when there are $-operator top-level keys' do
      let(:replacement) do
        { :$set => { a: 3 } }
      end

      it 'raises an exception' do
        expect do
          bulk.execute
        end.to raise_exception(Mongo::BulkWrite::InvalidReplacementDoc)
      end

    end

    context 'when a replace document is specified' do

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

      context 'when upsert is true' do

        let(:operations) do
          [ { replace_one: [ { a: 4 },
              replacement,
              { :upsert => true } ] } ]
        end

        let(:expected) do
          [ { 'a' => 1 }, { 'a' => 1 }, { 'a' => 2 } ]
        end

        it 'upserts the replacement document' do
          bulk.execute
          expect(authorized_collection.find(replacement).count).to eq(1)
        end

        it 'reports nMatched correctly' do
          expect(bulk.execute['nMatched']).to eq(0)
        end

        it 'does not replace any documents' do
          bulk.execute
          expect(authorized_collection.find.projection(_id: 0).to_a).to eq(expected)
        end
      end
    end
  end

  context 'update_one' do

    let(:docs) do
      [ { a: 1 }, { a: 1 } ]
    end

    let(:update) do
      { :$set => { a: 2 } }
    end

    let(:operations) do
      [ { update_one: [ { a: 1 },
                        update ] } ]
    end

    before do
      authorized_collection.insert_many(docs)
    end

    let(:expected) do
      [ { 'a' => 2 },  { 'a' => 1 } ]
    end

    context 'when an update document is not specified' do

      let(:operations) do
        [ { update_one: [ { a: 1 } ] } ]
      end

      it 'raises an exception' do
        expect do
          bulk.execute
        end.to raise_exception(ArgumentError)
      end
    end

    context 'when an invalid update document is specified' do

      let(:update) do
        { a: 2 }
      end

      it 'raises an exception' do
        expect do
          bulk.execute
        end.to raise_exception(Mongo::BulkWrite::InvalidUpdateDoc)
      end
    end

    context 'when a valid update document is specified' do

      it 'reports nModified correctly', if: write_command_enabled?  do
        expect(bulk.execute['nModified']).to eq(1)
      end

      it 'reports nModified correctly', unless: write_command_enabled?  do
        expect(bulk.execute['nModified']).to eq(nil)
      end

      it 'reports nUpserted correctly' do
        expect(bulk.execute['nUpserted']).to eq(0)
      end

      it 'reports nMatched correctly' do
        expect(bulk.execute['nMatched']).to eq(1)
      end

      it 'applies the correct writes' do
        bulk.execute
        expect(authorized_collection.find.projection(_id: 0).to_a).to eq(expected)
      end

      context 'when upsert is true' do

        let(:operations) do
          [ { update_one: [ { a: 3 },
                            update,
                            { upsert: true } ] } ]
        end

        let(:expected) do
          [ { 'a' => 1 },  { 'a' => 1 }, { 'a' => 2 } ]
        end

        it 'reports nModified correctly', if: write_command_enabled?  do
          expect(bulk.execute['nModified']).to eq(0)
        end

        it 'reports nModified correctly', unless: write_command_enabled?  do
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
  end

  context 'update_many' do

    let(:docs) do
      [ { a: 1 }, { a: 1 } ]
    end

    let(:update) do
      { :$set => { a: 2 } }
    end

    let(:operations) do
      [ { update_many: [ { a: 1 },
                        update ] } ]
    end

    let(:expected) do
      [ { 'a' => 2 },  { 'a' => 2 } ]
    end

    before do
      authorized_collection.insert_many(docs)
    end

    context 'when an update document is not specified' do

      let(:operations) do
        [ { update_many: [ { a: 1 } ] } ]
      end

      it 'raises an exception' do
        expect do
          bulk.execute
        end.to raise_exception(ArgumentError)
      end
    end

    context 'when an invalid update document is specified' do

      let(:update) do
        { a: 2 }
      end

      it 'raises an exception' do
        expect do
          bulk.execute
        end.to raise_exception(Mongo::BulkWrite::InvalidUpdateDoc)
      end
    end

    context 'when a valid update document is specified' do

      it 'reports nModified correctly', if: write_command_enabled?  do
        expect(bulk.execute['nModified']).to eq(2)
      end

      it 'reports nModified correctly', unless: write_command_enabled?  do
        expect(bulk.execute['nModified']).to eq(nil)
      end

      it 'reports nUpserted correctly' do
        expect(bulk.execute['nUpserted']).to eq(0)
      end

      it 'reports nMatched correctly' do
        expect(bulk.execute['nMatched']).to eq(2)
      end

      it 'applies the correct writes' do
        bulk.execute
        expect(authorized_collection.find.projection(_id: 0).to_a).to eq(expected)
      end

      context 'when upsert is true' do

        let(:operations) do
          [ { update_one: [ { a: 3 },
                            update,
                            { upsert: true } ] } ]
        end

        let(:expected) do
          [ { 'a' => 1 },  { 'a' => 1 }, { 'a' => 2 } ]
        end

        it 'reports nModified correctly', if: write_command_enabled?  do
          expect(bulk.execute['nModified']).to eq(0)
        end

        it 'reports nModified correctly', unless: write_command_enabled?  do
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
  end

  context 'when the operations need to be split' do

    before do
      authorized_collection.find.remove_many
      6000.times do |i|
        authorized_collection.insert_one(x: i)
      end
    end

    let(:operations) do
      [].tap do |ops|
        3000.times do |i|
          ops << { :update_one => [ { x: i },
                                    {'$set' => { x: 6000-i } }]
                 }
        end
        ops << { :insert_one => { test: 'emily' } }
        3000.times do |i|
          ops << { :update_one => [ { x: 3000+i },
                                    {'$set' => { x: 3000-i } }]
                 }
        end
      end
    end

    it 'completes all operations' do
      bulk.execute
      expect(authorized_collection.find(x: { '$lte' => 3000 }).to_a.size).to eq(3000)
    end
  end
end