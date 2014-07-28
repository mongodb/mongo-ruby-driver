shared_context 'indexed' do
  let(:spec)      { { :name => 1 } }
  let(:spec_name) { 'name_1' }
end

shared_examples 'an indexed collection' do

  before do
    c.drop_indexes
  end

  describe '#create_index' do

    before do
      c.create_index(spec)
    end

    it 'creates an index' do
      expect(c.stats['nindexes']).to eq(2)
    end

    context 'index options are used' do

      let(:opts)      { { :unique => true } }
      let(:opts_spec) { { :age => -1 } }
      let(:opts_name) { 'age_-1' }

      before do
        c.create_index(opts_spec, opts)
      end

      it 'creates an index' do
        expect(c.stats['nindexes']).to eq(3)
      end

      it 'creates an index with those options' do
        expect(c.index_information[opts_name]['unique']).to eq(true)
      end
    end
  end

  describe '#drop_index' do

    before do
      c.create_index(spec)
      c.create_index({ :age => -1 })
      c.insert({ :name => 'Harry', :age => 13 })
    end

    context 'when query matches a current index' do

      before do
        c.drop_index(spec_name)
      end

      it 'removes the specified index' do
        expect(c.stats['nindexes']).to eq(2)
        expect(c.stats['indexSizes'][spec_name]).to be(nil)
      end

      it 'leaves other indexes in the collection' do
        expect(c.stats['indexSizes']['age_-1']).to be_a(Integer)
      end

      it 'leaves indexed data in the collection' do
        expect(c.find_one({ :name => 'Harry' })['age']).to eq(13)
      end
    end

    context 'when query does not match any index' do

      before do
        c.drop_index('age_1_age_age_1')
      end

      it 'has no effect' do
        expect(c.stats['nindexes']).to eq(3)
      end
    end
  end

  describe '#drop_indexes' do

    before do
      c.create_index(spec)
      c.create_index({ :age => -1 })
      c.insert({ :name => 'Harry', :age => 13 })
      c.drop_indexes
    end

    it 'removes all custom indexes from the collection' do
      expect(c.stats['nindexes']).to eq(1)
    end

    it 'leaves indexed data in the collection' do
      expect(c.find_one({ :name => 'Harry' })['age']).to eq(13)
    end
  end

  describe '#ensure_index' do

    context 'the index is a new index' do

      it 'creates the index on the collection' do
        db.stub(:command)
        c.ensure_index(spec)
        expect(db).to have_received(:command)
      end
    end

    context 'the index has been applied recently' do

      let(:expiration) { Time.now.utc.to_i + 500 }
      let(:index)      { { spec_name => expiration } }

      before do
        c.client.index_cache(index, c.ns)
      end

      it 'does not re-create the index on the collection' do
        db.stub(:command)
        c.ensure_index(spec)
        expect(db).to_not have_received(:command)
      end
    end

    context 'the index was applied on this client, but not recently' do

      let(:expiration) { Time.now.utc.to_i - 9000 }
      let(:index)      { { spec_name => expiration } }

      before do
        c.client.index_cache(index, c.ns)
      end

      it 'creates the index on the collection' do
        db.stub(:command)
        c.ensure_index(spec)
        expect(db).to have_received(:command)
      end
    end

    context 'the index is new to this client but exists in the collection' do

      let(:new_client) { Mongo::Client.new(['127.0.0.1:27017'], :database => db_name) }
      let(:new_db)     { Mongo::Database.new(new_client, db_name) }
      let(:new_c)      { described_class.new(new_db, coll_name) }

      before do
        new_client.cluster.scan!
        new_c.ensure_index(spec)
      end

      it 'creates the index on the collection' do
        db.stub(:command)
        c.ensure_index(spec)
        expect(db).to have_received(:command)
      end
    end
  end

  describe '#index_information' do

    before do
      c.create_index(spec)
    end

    it 'returns a hash' do
      expect(c.index_information).to be_a(Hash)
    end

    it 'uses index names as keys in hash' do
      expect(c.index_information.keys.include?(spec_name)).to be(true)
    end

    it 'uses index information as values in hash' do
      expect(c.index_information[spec_name]).to be_a(Hash)
    end
  end
end
