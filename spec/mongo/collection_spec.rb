require 'spec_helper'

describe Mongo::Collection do
  include_context 'indexed'

  let(:db_name)     { 'test' }
  let(:client)      { Mongo::Client.new(['127.0.0.1:27017'], :database => db_name) }
  let(:coll_name)   { 'test-collection' }
  let(:db)          { Mongo::Database.new(client, db_name) }
  let(:c)           { described_class.new(db, coll_name) }
  let(:c_read)      { described_class.new(db, coll_name, {:read => :secondary}) }

  before do
    client.cluster.scan!
    c.insert({ :a => 1 })
    c.remove
  end

  it_behaves_like 'an indexed collection'

  describe '#==' do

    context 'when the names are the same' do

      context 'when the databases are the same' do

        let(:other) { described_class.new(db, coll_name) }

        it 'returns true' do
          expect(c).to eq(other)
        end
      end

      context 'when the databases are not the same' do

        let(:other_db) { Mongo::Database.new(client, :testing) }
        let(:other) { described_class.new(other_db, :users) }

        it 'returns false' do
          expect(c).to_not eq(other)
        end
      end
    end

    context 'when the names are not the same' do

      let(:other) { described_class.new(db, :sounds) }

      it 'returns false' do
        expect(c).to_not eq(other)
      end
    end

    context 'when the object is not a collection' do

      it 'returns false' do
        expect(c).to_not eq('test')
      end
    end
  end

  describe '#aggregate' do

    before do
      12.times do
        c.insert({ :name => 'Sam', :age => 23 })
      end
    end

    let(:group) { { "$group" => { :_id => "$age", :count => { "$sum" => 1 }}} }
    let(:match) { { "$match" => { :count => { "$gt" => 10 }}} }

    context 'pipeline is not an array' do

      it 'raises an error' do
        expect{c.aggregate(group)}.to raise_error
      end
    end

    context 'pipeline operators are not all hashes' do

      it 'raises an error' do
        expect{c.aggregate([ group, ["a"] ])}.to raise_error
      end
    end

    it 'returns an array' do
      expect(c.aggregate([ group, match ])).to be_a(Array)
    end

    context 'the cursor option is passed' do

      it 'returns a cursor' do
        expect(c.aggregate([ group, match ], {:cursor => {}})).to be_a(Mongo::Cursor)
      end
    end
  end

  describe '#capped?' do

    let(:capped_name) { 'cc' }
    let(:capped_opts) { { :capped => true, :size => 512 } }

    context 'when collection is capped' do

      let(:capped) { described_class.new(db, capped_name, capped_opts) }

      context 'when collection did not already exist in database' do

        it 'returns true' do
          # expect(capped.capped?).to eq(true)
        end
      end

      context 'when collection already existed in database' do

        #before { capped.insert({ :a => 1 }) }

        context 'when capped is passed as option' do

          it 'returns true' do
            # cc = described_class.new(db, capped_name, capped_opts)
            # expect(cc.capped?).to eq(true)
          end
        end

        context 'when capped is not passed as option' do

          it 'returns true' do
            # cc = described_class.new(db, capped_name)
            # expect(cc.capped?).to eq(true)
          end
        end
      end
    end

    context 'when collection is not capped' do

      context 'when collection already existed in database' do

        before do
          c.insert({ :a => 1 })
        end

        context 'when options include capped' do

          let(:c) { described_class.new(db, coll_name, capped_opts) }

          it 'returns false' do
            # @todo - should this raise an error?
            # expect(c.capped?).to eq(false)
          end
        end

        context 'when options do not include capped' do

          let(:c) { described_class.new(db, coll_name) }

          it 'returns false' do
            # expect(c.capped?).to eq(false)
          end
        end
      end

      context 'when collection did not exist in database' do

        it 'returns false' do
          # expect(c.capped?).to eq(false)
        end
      end
    end
  end

  describe '#count' do

    let(:num_docs) { 10 }

    before do
      num_docs.times do |i|
        c.insert({ :a => i })
      end
    end

    context 'when no options are given' do

      it 'returns the number of documents in the collection' do
        expect(c.count).to eq(num_docs)
      end
    end

    context 'when options are given' do

      it 'returns the number of documents matching the query' do
        expect(c.count({:query => { :a => { "$gt" => 4 }}})).to eq(num_docs - 5)
      end

      it 'returns no more than the specified limit' do
        expect(c.count({:limit => 3})).to eq(3)
      end

      it 'returns the number of matching documents minus the number to skip' do
        expect(c.count({:skip => 2})).to eq(num_docs - 2)
      end
    end
  end

  describe '#distinct' do

    let(:num_docs) { 10 }

    before do
      num_docs.times do |i|
        c.insert({ :a => i, :b => 0 })
        c.insert({ :a => i, :b => 1 })
      end
    end

    it 'returns an array' do
      expect(c.distinct(:a)).to be_a(Array)
    end

    it 'returns an array the length of the number of distinct values' do
      expect(c.distinct(:a).length).to eq(num_docs)
    end

    it 'returns an array of the distinct values' do
      values = c.distinct(:a)
      num_docs.times do |i|
        expect(values.include?(i)).to be(true)
      end
    end
  end

  describe '#drop' do

    before do
      c.insert({ :name => 'Patrick' })
      c.create_index({ :name => 1 })
      c.drop
    end

    it 'removes all documents from this collection' do
      expect(c.count).to eq(0)
    end

    it 'removes all indexes from this collection' do
      expect(c.stats["nindexes"]).to eq(nil)
    end

    it 'removes the collection from this database' do
      expect(db.collection_names.include?(coll_name)).to eq(false)
    end
  end

  describe '#find' do

    let(:find_opts) do
      { :read => :secondary,
        :sort => [[ :name, Mongo::ASCENDING ]],
        :limit => 1 }
    end

    before do
      c.insert({ :a => 1 })
    end

    it 'returns a CollectionView' do
      expect(c.find({:a => 1})).to be_a(Mongo::CollectionView)
    end

    it 'returns a CollectionView with the correct spec' do
      expect(c.find({:a => 1}).selector).to eq({:a => 1})
    end

    it 'returns a CollectionView with the right options' do
      expect(c.find({}, find_opts).opts).to eq(find_opts)
    end

    context 'timeout set to false without a block given' do

      it 'raises an error' do
        expect{c.find({}, { :timeout => false })}.to raise_error
      end
    end

    context 'block given' do

      it 'yields to block with each result document' do
        # @todo - implement insert first
      end
    end

    context 'when named_hint is given' do

      it 'passes named_hint as hint to the collection view' do
        expect(c.find({}, {:named_hint => 'age_1'}).opts[:hint]).to eq('age_1')
      end
    end

    context 'when both named_hint and hint are given' do

      let(:hint_opts) { { :hint => 'name', :named_hint => 'age_1' } }

      it 'gives hint preference' do
        expect(c.find({}, hint_opts).opts[:hint]).to eq('name')
      end
    end

    context 'when no read preference is given' do

      context 'when the collection has a read preference set' do

        it 'gives the collection-level read preference' do
          # @todo - server preference on collection view
          # expect(c_read.find({}).opts[:read]).to eq(:secondary)
        end
      end

      context 'when no collection-level read preference exists' do

        it 'gives no read preference' do
          # @todo - server preference on collection view
          # expect(c.find({}).opts[:read]).to eq(nil)
        end
      end
    end

    context 'when a read preference is given' do

      context 'when the collection has a read preference' do

        it 'gives the query-level read preference' do
          # @todo - server preference on collection view
          # expect(c_read.find({}, {:read => :primary}).opts[:read]).to eq(:primary)
        end
      end

      context 'when no collection-level read preference is set' do

        it 'gives the query-level read preference' do
          # @todo - server preference on collection view
          # expect(c.find({}, {:read => :primary}).opts[:read]).to eq(:primary)
        end
      end
    end

    # @todo - many more test cases for options!
  end

  describe '#find_one' do

    let(:id)       { BSON::ObjectId.new }
    let(:doc)      { { :_id => id, :name => 'Sam' } }
    let(:selector) { { :name => 'Sam' } }

    before do
      c.insert(doc)
    end

    context 'when a hash selector is given' do

      it 'returns a single document' do
        expect(c.find_one(selector)).to be_a(Hash)
      end

      it 'returns a single document that matches the query' do
        expect(c.find_one(selector)['name']).to eq('Sam')
      end

      it 'returns nil when there are no matching documents' do
        expect(c.find_one({ :name => 'Leo' })).to eq(nil)
      end
    end

    context 'when a BSON::ObjectId is given' do

      it 'returns a single document' do
        c.remove
        c.insert(doc)
        expect(c.find_one(id)).to be_a(Hash)
      end

      it 'returns a single document that matches the query' do
        expect(c.find_one(id)['name']).to eq('Sam')
      end

      it 'returns nil when there are no matching documents' do
        expect(c.find_one(BSON::ObjectId.new)).to eq(nil)
      end
    end
  end

  describe '#insert' do

    context 'one document is given' do

      it 'returns a single id' do
        expect(c.insert({ :name => 'Phil' })).to be_a(BSON::ObjectId)
      end

      context 'a pk_factory is specified' do

        let(:pk)      { pk_factory(:plain) }
        let(:pk_coll) { described_class.new(db, "pkcoll", { :pk => pk }) }

        it 'inserts documents using custom primary keys' do
          expect(pk_coll.insert({ :name => 'Dave' })).to be_a(Integer)
        end
      end
    end

    context 'multiple documents are given' do

      let(:docs) { [{:name => 'Kyle'}, {:name => 'Amalia'}, {:name => 'Luke'}] }

      it 'inserts all of the documents' do
        c.insert(docs)
        expect(c.count).to eq(3)
      end

      it 'returns an array of ids' do
        res = c.insert(docs)
        expect(res).to be_a(Array)
        expect(res.length).to eq(docs.length)
      end

      context ':continue_on_error is true' do

        context 'writes are not acknowledged' do

          let(:dup_pk)  { pk_factory(:duplicate, { :dups => true }) }
          let(:pk_coll) { described_class.new(db, "pkcoll",
                                              { :pk => dup_pk,
                                                :continue_on_error => true,
                                                :w => 0 }) }

          it 'attempts to insert all the documents' do
            res = pk_coll.insert(docs)
            expect(res).to be_a(Array)
            expect(res.length).to eq(docs.length)
          end
        end

        context 'writes are acknowledged' do

          let(:coll_pk_w) { described_class.new(db, "pkwithw",
                                                { :pk => dup_pk,
                                                  :continue_on_error => true,
                                                  :w => 1 }) }

          it 'raises an error on failure' do
            expect{coll_pk_w.insert(docs)}.to raise_error
          end
        end
      end

      context ':continue_on_error is false' do

        # @todo, what exactly is the intended behaviour here?
      end
    end
  end

  describe '#parallel_scan' do

    before do
      1000.times do |i|
        c.insert({ :a => i })
      end
    end

    it 'returns an array of cursors' do
      expect(c.parallel_scan(3)).to be_a(Array)
      expect(c.parallel_scan(3).length).to eq(3)
      expect(c.parallel_scan(3).first).to be_a(Mongo::Cursor)
    end

    it 'returns cursors that cover all documents in the collection' do
      docs = 0
      c.parallel_scan(3).each do |cursor|
        # @todo - is a Cursor enumerable?
        # docs += cursor.count
      end
      # expect(docs).to eq(1000)
    end
  end

  describe '#rename' do

    context 'new name is invalid' do

      let(:newname) { '$$aaah.'}

      before do
        c.insert({ :a => 1 })
      end

      it 'raises an error' do
        expect{c.rename(newname)}.to raise_error
      end

      it 'does not change the collection name attribute' do
        expect{c.rename(newname)}.to raise_error
        expect(c.name).to eq(coll_name)
      end

      it 'does not change the collection name in the db' do
        expect{c.rename(newname)}.to raise_error
        expect(db.collection_names.include?(coll_name)).to eq(true)
        expect(db.collection_names.include?(newname)).to eq(false)
      end
    end

    context 'new name is valid' do

      let(:newname) { 'sally' }

      before do
        c.insert({ :a => 1 })
        c.rename(newname)
      end

      it 'changes the collection name attribute' do
        expect(c.name).to eq(newname)
      end

      it 'changes the collection name in the db' do
        expect(db.collection_names.include?(coll_name)).to eq(false)
        expect(db.collection_names.include?(newname)).to eq(true)
      end
    end
  end

  describe '#update' do

    before do
      c.insert({ :name => 'Faith', :age => 23 })
      c.insert({ :name => 'Sam', :age => 23 })
    end

    context 'no options are passed' do

      context 'matching documents exist' do

        before do
          c.update({ :age => 23 }, { "$set" => { :age => 24 }})
        end

        it 'updates a single matching document' do
          expect(c.count({ :query => { :age => 23 }})).to eq(1)
          expect(c.count({ :query => { :age => 24 }})).to eq(1)
        end
      end

      context 'no matching documents exist' do

        before do
          c.update({ :age => 25 }, { "$set" => { :age => 26 }})
        end

        it 'has no effect' do
          expect(c.count({ :query => { :age => 26 }})).to eq(0)
        end
      end
    end

    context 'multi is true' do

      context 'matching documents exist' do

        before do
          c.update({ :age => 23 }, { "$set" => { :age => 24 }}, { :multi => true })
        end

        it 'updates all matching documents' do
          expect(c.count({ :query => { :age => 23 }})).to eq(0)
          expect(c.count({ :query => { :age => 24 }})).to eq(2)
        end
      end

      context 'no matching documents exist' do

        before do
          c.update({ :age => 25 }, { "$set" => { :age => 26 }}, { :multi => true })
        end

        it 'has no effect' do
          expect(c.count({ :query => { :age => 26 }})).to eq(0)
        end
      end
    end

    context 'upsert is true' do

      context 'matching documents exist' do

        before do
          c.update({ :age => 23 }, { "$set" => { :age => 24 }}, { :upsert => true })
        end

        it 'updates a single matching document' do
          expect(c.count({ :query => { :age => 23 }})).to eq(1)
          expect(c.count({ :query => { :age => 24 }})).to eq(1)
        end
      end

      context 'no matching documents exist' do

        before do
          c.update({ :age => 25 }, { "$set" => { :age => 26 }}, { :upsert => true })
        end

        it 'inserts a new document' do
          expect(c.count({ :query => { :age => 26 }})).to eq(1)
        end
      end
    end

    context 'multi and upsert are both true' do

      context 'matching documents exist' do

        before do
          c.update({ :age => 23 },
                   { "$set" => { :age => 24 }},
                   { :upsert => true,
                     :multi => true })
        end

        it 'updates all matching documents' do
          expect(c.count({ :query => { :age => 23 }})).to eq(0)
          expect(c.count({ :query => { :age => 24 }})).to eq(2)
        end
      end

      context 'no matching documents exist' do

        before do
          c.update({ :age => 25 },
                   { "$set" => { :age => 26 }},
                   { :upsert => true,
                     :multi => true })
        end

        it 'inserts a single new document' do
          expect(c.count({ :query => { :age => 23 }})).to eq(2)
          expect(c.count({ :query => { :age => 26 }})).to eq(1)
        end
      end
    end
  end

  describe '.validate_name' do

    context 'name is empty' do

      it 'raises an error' do
        expect{described_class.validate_name('')}.to raise_error
      end
    end

    context 'name contains ..' do

      it 'raises an error' do
        expect{described_class.validate_name('hey..there')}.to raise_error
      end
    end

    context 'name contains $' do

      it 'raises an error' do
        expect{described_class.validate_name('dollah$dollah$billz')}.to raise_error
      end
    end

    context 'name begins with .' do

      it 'raises an error' do
        expect{described_class.validate_name('.dotdot')}.to raise_error
      end
    end

    context 'name ends with .' do

      it 'raises an error' do
        expect{described_class.validate_name('dotdot.')}.to raise_error
      end
    end

    context 'name contains a null character' do

      it 'raises an error' do
        expect{described_class.validate_name('bye\0bye')}.to raise_error
      end
    end
  end

  describe '#remove' do

    let(:num_docs) { 10 }

    before do
      num_docs.times do |i|
        c.insert({ :a => i })
      end
    end

    context 'no selector is given' do

      before do
        c.remove
      end

      it 'does removes all documents in the collection' do
        expect(c.count).to eq(0)
      end
    end

    context 'a selector is given' do

      context 'no options are given' do

        before do
          c.remove({ :a => { "$gt" => 4 }})
        end

        it 'removes all matching documents from the collection' do
          expect(c.count({})).to eq(num_docs - 5)
        end
      end

      context 'options are given' do

        context 'limit is 1' do

          before do
            c.remove({ :a => { "$gt" => 4 }}, { :limit => 1 })
          end

          it 'removes just one matching document' do
            expect(c.count({})).to eq(num_docs - 1)
          end
        end

        context 'limit is 0' do

          it 'removes all matching documents' do
            c.remove({ :a => { "$gt" => 4 }}, { :limit => 0 })
            expect(c.count({})).to eq(num_docs - 5)
          end
        end

        context 'limit is > 1' do

          it 'raises an error' do
            expect{c.remove({ :a => 1 }, { :limit => 4 })}.to raise_error
          end
        end

#        it 'runs with the proper write concern' do
#        end

        context 'both fsync and j are set' do

          it 'raises an error' do
            expect{c.remove({:a => 1}, {:j => true, :fsync => true})}.to raise_error
          end
        end
      end
    end
  end

  describe '#save' do

    context 'document has an _id field' do

      context 'document with that _id already exists' do

        before do
          c.insert({ :_id => 1, :name => "red fish" })
        end

        it 'replaces the existing document' do
          c.save({ :_id => 1, :name => "blue fish" })
          expect(c.count({ :name => "blue fish" })).to eq(1)
          expect(c.count({ :name => "red fish" })).to eq(0)
        end
      end

      context 'there is no existing document with that _id' do

        it 'inserts the new document' do
          c.save({ :_id => 1, :name => "two fish" })
          expect(c.count({ :name => "two fish" })).to eq(1)
        end
      end
    end

    context 'document has no _id field' do

      before do
        c.insert({ :name => "one fish" })
        c.save({ :name => "one fish" })
      end

      it 'inserts the new document' do
        expect(c.count({ :name => "one fish" })).to eq(2)
      end
    end
  end

  describe '#stats' do

    it 'returns a hash' do
      expect(c.stats).to be_a(Hash)
    end

    it 'returns stats on this collection' do
      expect(c.stats['ns']).to eq("#{db_name}.#{coll_name}")
    end
  end
end
