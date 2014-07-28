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
      end
    end

    context 'the index has been applied recently' do

      it 'does not create the index on the collection' do
      end
    end

    context 'the index was applied, but not recently' do

      it 'creates the index on the collection' do
      end
    end

    context 'the index is new to us but exists in the collection' do

      it 'creates the index on the collection' do
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
