$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'
require 'test/unit'

# NOTE: assumes Mongo is running
class DBAPITest < Test::Unit::TestCase
  include XGen::Mongo
  include XGen::Mongo::Driver

  @@db = Mongo.new(ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost',
                   ENV['MONGO_RUBY_DRIVER_PORT'] || Mongo::DEFAULT_PORT).db('ruby-mongo-test')
  @@coll = @@db.collection('test')

  def setup
    @@coll.clear
    @r1 = @@coll.insert('a' => 1) # collection not created until it's used
    @@coll_full_name = 'ruby-mongo-test.test'
  end

  def teardown
    @@coll.clear
  end

  def test_clear
    assert_equal 1, @@coll.count
    @@coll.clear
    assert_equal 0, @@coll.count
  end

  def test_insert
    @@coll.insert('a' => 2)
    @@coll.insert('b' => 3)

    assert_equal 3, @@coll.count
    docs = @@coll.find().to_a
    assert_equal 3, docs.length
    assert docs.detect { |row| row['a'] == 1 }
    assert docs.detect { |row| row['a'] == 2 }
    assert docs.detect { |row| row['b'] == 3 }

    @@coll << {'b' => 4}
    docs = @@coll.find().to_a
    assert_equal 4, docs.length
    assert docs.detect { |row| row['b'] == 4 }
  end

  def test_insert_multiple
    @@coll.insert({'a' => 2}, {'b' => 3})

    assert_equal 3, @@coll.count
    docs = @@coll.find().to_a
    assert_equal 3, docs.length
    assert docs.detect { |row| row['a'] == 1 }
    assert docs.detect { |row| row['a'] == 2 }
    assert docs.detect { |row| row['b'] == 3 }
  end

  def test_find_simple
    @r2 = @@coll.insert('a' => 2)
    @r3 = @@coll.insert('b' => 3)
    # Check sizes
    docs = @@coll.find().to_a
    assert_equal 3, docs.size
    assert_equal 3, @@coll.count

    # Find by other value
    docs = @@coll.find('a' => @r1['a']).to_a
    assert_equal 1, docs.size
    doc = docs.first
    # Can't compare _id values because at insert, an _id was added to @r1 by
    # the database but we don't know what it is without re-reading the record
    # (which is what we are doing right now).
#     assert_equal doc['_id'], @r1['_id']
    assert_equal doc['a'], @r1['a']
  end

  def test_find_advanced
    @@coll.insert('a' => 2)
    @@coll.insert('b' => 3)

    # Find by advanced query (less than)
    docs = @@coll.find('a' => { '$lt' => 10 }).to_a
    assert_equal 2, docs.size
    assert docs.detect { |row| row['a'] == 1 }
    assert docs.detect { |row| row['a'] == 2 }

    # Find by advanced query (greater than)
    docs = @@coll.find('a' => { '$gt' => 1 }).to_a
    assert_equal 1, docs.size
    assert docs.detect { |row| row['a'] == 2 }

    # Find by advanced query (less than or equal to)
    docs = @@coll.find('a' => { '$lte' => 1 }).to_a
    assert_equal 1, docs.size
    assert docs.detect { |row| row['a'] == 1 }

    # Find by advanced query (greater than or equal to)
    docs = @@coll.find('a' => { '$gte' => 1 }).to_a
    assert_equal 2, docs.size
    assert docs.detect { |row| row['a'] == 1 }
    assert docs.detect { |row| row['a'] == 2 }

    # Find by advanced query (between)
    docs = @@coll.find('a' => { '$gt' => 1, '$lt' => 3 }).to_a
    assert_equal 1, docs.size
    assert docs.detect { |row| row['a'] == 2 }

    # Find by advanced query (in clause)
    docs = @@coll.find('a' => {'$in' => [1,2]}).to_a
    assert_equal 2, docs.size
    assert docs.detect { |row| row['a'] == 1 }
    assert docs.detect { |row| row['a'] == 2 }

    # Find by advanced query (regexp)
    docs = @@coll.find('a' => /[1|2]/).to_a
    assert_equal 2, docs.size
    assert docs.detect { |row| row['a'] == 1 }
    assert docs.detect { |row| row['a'] == 2 }
  end

  def test_find_sorting
    @@coll.clear
    @@coll.insert('a' => 1, 'b' => 2)
    @@coll.insert('a' => 2, 'b' => 1)
    @@coll.insert('a' => 3, 'b' => 2)
    @@coll.insert('a' => 4, 'b' => 1)

    # Sorting (ascending)
    docs = @@coll.find({'a' => { '$lt' => 10 }}, :sort => {'a' => 1}).to_a
    assert_equal 4, docs.size
    assert_equal 1, docs[0]['a']
    assert_equal 2, docs[1]['a']
    assert_equal 3, docs[2]['a']
    assert_equal 4, docs[3]['a']

    # Sorting (descending)
    docs = @@coll.find({'a' => { '$lt' => 10 }}, :sort => {'a' => -1}).to_a
    assert_equal 4, docs.size
    assert_equal 4, docs[0]['a']
    assert_equal 3, docs[1]['a']
    assert_equal 2, docs[2]['a']
    assert_equal 1, docs[3]['a']

    # Sorting using array of names; assumes ascending order.
    docs = @@coll.find({'a' => { '$lt' => 10 }}, :sort => ['a']).to_a
    assert_equal 4, docs.size
    assert_equal 1, docs[0]['a']
    assert_equal 2, docs[1]['a']
    assert_equal 3, docs[2]['a']
    assert_equal 4, docs[3]['a']

    # Sorting using single name; assumes ascending order.
    docs = @@coll.find({'a' => { '$lt' => 10 }}, :sort => 'a').to_a
    assert_equal 4, docs.size
    assert_equal 1, docs[0]['a']
    assert_equal 2, docs[1]['a']
    assert_equal 3, docs[2]['a']
    assert_equal 4, docs[3]['a']

    docs = @@coll.find({'a' => { '$lt' => 10 }}, :sort => ['b', 'a']).to_a
    assert_equal 4, docs.size
    assert_equal 2, docs[0]['a']
    assert_equal 4, docs[1]['a']
    assert_equal 1, docs[2]['a']
    assert_equal 3, docs[3]['a']

    # Sorting using empty array; no order guarantee (Mongo bug #898) but
    # should not blow up.
    docs = @@coll.find({'a' => { '$lt' => 10 }}, :sort => []).to_a
    assert_equal 4, docs.size

    # Sorting using array of hashes; no order guarantee (Mongo bug #898) but
    # should not blow up.
    docs = @@coll.find({'a' => { '$lt' => 10 }}, :sort => [{'b' => 1}, {'a' => -1}]).to_a
    assert_equal 4, docs.size

    # Sorting using ordered hash. You can use an unordered one, but then the
    # order of the keys won't be guaranteed thus your sort won't make sense.
    oh = OrderedHash.new
    oh['a'] = -1
    docs = @@coll.find({'a' => { '$lt' => 10 }}, :sort => oh).to_a
    assert_equal 4, docs.size
    assert_equal 4, docs[0]['a']
    assert_equal 3, docs[1]['a']
    assert_equal 2, docs[2]['a']
    assert_equal 1, docs[3]['a']

    # TODO this will not pass due to known Mongo bug #898
#     oh = OrderedHash.new
#     oh['b'] = -1
#     oh['a'] = 1
#     docs = @@coll.find({'a' => { '$lt' => 10 }}, :sort => oh).to_a
#     assert_equal 4, docs.size
#     assert_equal 1, docs[0]['a']
#     assert_equal 3, docs[1]['a']
#     assert_equal 2, docs[2]['a']
#     assert_equal 4, docs[3]['a']
  end

  def test_find_limits
    @@coll.insert('b' => 2)
    @@coll.insert('c' => 3)
    @@coll.insert('d' => 4)

    docs = @@coll.find({}, :limit => 1).to_a
    assert_equal 1, docs.size
    docs = @@coll.find({}, :limit => 2).to_a
    assert_equal 2, docs.size
    docs = @@coll.find({}, :limit => 3).to_a
    assert_equal 3, docs.size
    docs = @@coll.find({}, :limit => 4).to_a
    assert_equal 4, docs.size
    docs = @@coll.find({}).to_a
    assert_equal 4, docs.size
    docs = @@coll.find({}, :limit => 99).to_a
    assert_equal 4, docs.size
  end

  def test_find_first
    x = @@coll.find_first('a' => 1)
    assert_not_nil x
    assert_equal 1, x['a']
  end

  def test_find_first_no_records
    @@coll.clear
    x = @@coll.find_first('a' => 1)
    assert_nil x
  end

  def test_drop_collection
    assert @@db.drop_collection(@@coll.name), "drop of collection #{@@coll.name} failed"
    assert !@@db.collection_names.include?(@@coll_full_name)
  end

  def test_other_drop
    assert @@db.collection_names.include?(@@coll_full_name)
    @@coll.drop
    assert !@@db.collection_names.include?(@@coll_full_name)
  end

  def test_collection_names
    names = @@db.collection_names
    assert names.length >= 1
    assert names.include?(@@coll_full_name)

    coll2 = @@db.collection('test2')
    coll2.insert('a' => 1)      # collection not created until it's used
    names = @@db.collection_names
    assert names.length >= 2
    assert names.include?(@@coll_full_name)
    assert names.include?('ruby-mongo-test.test2')
  ensure
    @@db.drop_collection('test2')
  end

  def test_collections_info
    cursor = @@db.collections_info
    rows = cursor.to_a
    assert rows.length >= 1
    row = rows.detect { |r| r['name'] == @@coll_full_name }
    assert_not_nil row
  end

  def test_collection_options
    @@db.drop_collection('foobar')
    @@db.strict = true

    begin
      coll = @@db.create_collection('foobar', :capped => true, :size => 1024)
      options = coll.options()
      assert_equal 'foobar', options['create']
      assert_equal true, options['capped']
      assert_equal 1024, options['size']
    rescue => ex
      @@db.drop_collection('foobar')
      fail "did not expect exception \"#{ex}\""
    ensure
      @@db.strict = false
    end
  end

  def test_index_information
    name = @@db.create_index(@@coll.name, 'a')
    list = @@db.index_information(@@coll.name)
    assert_equal @@coll.index_information, list
    assert_equal 1, list.length

    info = list[0]
    assert_equal name, 'a_1'
    assert_equal name, info[:name]
    assert_equal 1, info[:keys]['a']
  ensure
    @@db.drop_index(@@coll.name, name)
  end

  def test_multiple_index_cols
    name = @@db.create_index(@@coll.name, [['a', DESCENDING], ['b', ASCENDING], ['c', DESCENDING]])
    list = @@db.index_information(@@coll.name)
    assert_equal 1, list.length

    info = list[0]
    assert_equal name, 'a_-1_b_1_c_-1'
    assert_equal name, info[:name]
    keys = info[:keys].keys
    assert_equal ['a', 'b', 'c'], keys.sort
  ensure
    @@db.drop_index(@@coll.name, name)
  end

  def test_array
    @@coll << {'b' => [1, 2, 3]}
    rows = @@coll.find({}, {:fields => ['b']}).to_a
    assert_equal 1, rows.length
    assert_equal [1, 2, 3], rows[0]['b']
  end

  def test_regex
    regex = /foobar/i
    @@coll << {'b' => regex}
    rows = @@coll.find({}, {:fields => ['b']}).to_a
    assert_equal 1, rows.length
    assert_equal regex, rows[0]['b']
  end

  def test_non_oid_id
    # Note: can't use Time.new because that will include fractional seconds,
    # which Mongo does not store.
    t = Time.at(1234567890)
    @@coll << {'_id' => t}
    rows = @@coll.find({'_id' => t}).to_a
    assert_equal 1, rows.length
    assert_equal t, rows[0]['_id']
  end

  def test_strict
    assert !@@db.strict?
    @@db.strict = true
    assert @@db.strict?
  ensure
    @@db.strict = false
  end

  def test_strict_access_collection
    @@db.strict = true
    begin
      @@db.collection('does-not-exist')
      fail "expected exception"
    rescue => ex
      assert_equal "Collection does-not-exist doesn't exist. Currently in strict mode.", ex.to_s
    ensure
      @@db.strict = false
      @@db.drop_collection('does-not-exist')
    end
  end

  def test_strict_create_collection
    @@db.drop_collection('foobar')
    @@db.strict = true

    begin
      @@db.create_collection('foobar')
      assert true
    rescue => ex
      fail "did not expect exception \"#{ex}\""
    end

    # Now the collection exists. This time we should see an exception.
    begin
      @@db.create_collection('foobar')
      fail "expected exception"
    rescue => ex
      assert_equal "Collection foobar already exists. Currently in strict mode.", ex.to_s
    ensure
      @@db.strict = false
      @@db.drop_collection('foobar')
    end

    # Now we're not in strict mode - should succeed
    @@db.create_collection('foobar')
    @@db.create_collection('foobar')
    @@db.drop_collection('foobar')
  end

  def test_replace
    assert_equal @@coll.count, 1
    assert_equal @@coll.find_first["a"], 1

    @@coll.replace({"a" => 1}, {"a" => 2})
    assert_equal @@coll.count, 1
    assert_equal @@coll.find_first["a"], 2

    @@coll.replace({"b" => 1}, {"a" => 3})
    assert_equal @@coll.count, 1
    assert_equal @@coll.find_first["a"], 2
  end

  def test_repsert
    assert_equal @@coll.count, 1
    assert_equal @@coll.find_first["a"], 1

    @@coll.repsert({"a" => 1}, {"a" => 2})
    assert_equal @@coll.count, 1
    assert_equal @@coll.find_first["a"], 2

    @@coll.repsert({"b" => 1}, {"a" => 3})
    assert_equal @@coll.count, 2
    assert @@coll.find_first({"a" => 3})
  end

  def test_to_a
    cursor = @@coll.find()
    rows = cursor.to_a

    # Make sure we get back exactly the same array the next time we ask
    rows2 = cursor.to_a
    assert_same rows, rows2

    # Make sure we can still iterate after calling to_a
    rows_with_each = cursor.collect{|row| row}
    assert_equal rows, rows_with_each

    # Make sure we can iterate more than once after calling to_a
  end

  def test_to_a_after_each
    cursor = @@coll.find
    cursor.each { |row| row }
    begin
      cursor.to_a
      fail "expected \"can't call\" error"
    rescue => ex
      assert_equal "can't call Cursor#to_a after calling Cursor#each", ex.to_s
    end
  end

  def test_ismaster
    assert @@db.master?
  end

  def test_master
    assert_equal "#{@@db.host}:#{@@db.port}", @@db.master
  end

  def test_hint
    name = @@coll.create_index('a')
    begin
      assert_nil @@coll.hint
      assert_equal 1, @@coll.find({'a' => 1}, :hint => 'a').to_a.size
      assert_equal 1, @@coll.find({'a' => 1}, :hint => ['a']).to_a.size
      assert_equal 1, @@coll.find({'a' => 1}, :hint => {'a' => 1}).to_a.size

      @@coll.hint = 'a'
      assert_equal({'a' => 1}, @@coll.hint)
      assert_equal 1, @@coll.find('a' => 1).to_a.size

      @@coll.hint = ['a']
      assert_equal({'a' => 1}, @@coll.hint)
      assert_equal 1, @@coll.find('a' => 1).to_a.size

      @@coll.hint = {'a' => 1}
      assert_equal({'a' => 1}, @@coll.hint)
      assert_equal 1, @@coll.find('a' => 1).to_a.size

      @@coll.hint = nil
      assert_nil @@coll.hint
      assert_equal 1, @@coll.find('a' => 1).to_a.size
    ensure
      @@coll.drop_index(name)
    end
  end

# TODO this test fails with error message "Undefed Before end of object"
# That is a database error. The undefined type may go away.

#   def test_insert_undefined
#     doc = {'undef' => Undefined.new}
#     @@coll.clear
#     @@coll.insert(doc)
#     p @@db.error                 # DEBUG
#     assert_equal 1, @@coll.count
#     row = @@coll.find().next_object
#     assert_not_nil row
#   end

end
