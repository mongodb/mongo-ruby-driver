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
end
