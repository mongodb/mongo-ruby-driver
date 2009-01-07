$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'
require 'test/unit'

# NOTE: assumes Mongo is running
class DBAPITest < Test::Unit::TestCase

  include XGen::Mongo::Driver

  def setup
    host = ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost'
    port = ENV['MONGO_RUBY_DRIVER_PORT'] || Mongo::DEFAULT_PORT
    @db = Mongo.new(host, port).db('ruby-mongo-test')
    @coll = @db.collection('test')
    @coll.clear
    @r1 = @coll.insert('a' => 1) # collection not created until it's used
    @coll_full_name = 'ruby-mongo-test.test'
  end

  def teardown
    @coll.clear unless @coll == nil || @db.socket.closed?
  end

  def test_clear
    assert_equal 1, @coll.count
    @coll.clear
    assert_equal 0, @coll.count
  end

  def test_insert
    @coll.insert('a' => 2)
    @coll.insert('b' => 3)

    assert_equal 3, @coll.count
    docs = @coll.find().map{ |x| x }
    assert_equal 3, docs.length
    assert docs.detect { |row| row['a'] == 1 }
    assert docs.detect { |row| row['a'] == 2 }
    assert docs.detect { |row| row['b'] == 3 }

    @coll << {'b' => 4}
    docs = @coll.find().map{ |x| x }
    assert_equal 4, docs.length
    assert docs.detect { |row| row['b'] == 4 }
  end
  
  def test_find_simple
    @r2 = @coll.insert('a' => 2)
    @r3 = @coll.insert('b' => 3)
    # Check sizes
    docs = @coll.find().map{ |x| x }
    assert_equal 3, docs.size
    assert_equal 3, @coll.count

    # Find by other value
    docs = @coll.find('a' => @r1['a']).map{ |x| x }
    assert_equal 1, docs.size
    doc = docs.first
    assert_equal doc['_id'], @r1['_id']
    assert_equal doc['a'], @r1['a']
  end

  def test_find_advanced
    @coll.insert('a' => 2)
    @coll.insert('b' => 3)

    # Find by advanced query (less than)
    docs = @coll.find('a' => { '$lt' => 10 }).map{ |x| x }
    assert_equal 2, docs.size
    assert docs.detect { |row| row['a'] == 1 }
    assert docs.detect { |row| row['a'] == 2 }

    # Find by advanced query (greater than)
    docs = @coll.find('a' => { '$gt' => 1 }).map{ |x| x }
    assert_equal 1, docs.size
    assert docs.detect { |row| row['a'] == 2 }

    # Find by advanced query (less than or equal to)
    docs = @coll.find('a' => { '$lte' => 1 }).map{ |x| x }
    assert_equal 1, docs.size
    assert docs.detect { |row| row['a'] == 1 }

    # Find by advanced query (greater than or equal to)
    docs = @coll.find('a' => { '$gte' => 1 }).map{ |x| x }
    assert_equal 2, docs.size
    assert docs.detect { |row| row['a'] == 1 }
    assert docs.detect { |row| row['a'] == 2 }

    # Find by advanced query (between)
    docs = @coll.find('a' => { '$gt' => 1, '$lt' => 3 }).map{ |x| x }
    assert_equal 1, docs.size
    assert docs.detect { |row| row['a'] == 2 }

    # Find by advanced query (in clause)
    docs = @coll.find('a' => {'$in' => [1,2]}).map{ |x| x }
    assert_equal 2, docs.size
    assert docs.detect { |row| row['a'] == 1 }
    assert docs.detect { |row| row['a'] == 2 }

    # Find by advanced query (regexp)
    docs = @coll.find('a' => /[1|2]/).map{ |x| x }
    assert_equal 2, docs.size
    assert docs.detect { |row| row['a'] == 1 }
    assert docs.detect { |row| row['a'] == 2 }
  end

  def test_find_sorting
    @coll.clear
    @coll.insert('a' => 1, 'b' => 2)
    @coll.insert('a' => 2, 'b' => 1)
    @coll.insert('a' => 3, 'b' => 2)
    @coll.insert('a' => 4, 'b' => 1)

    # Sorting (ascending)
    docs = @coll.find({'a' => { '$lt' => 10 }}, :sort => [{'a' => 1}]).map{ |x| x }
    assert_equal 4, docs.size
    assert_equal 1, docs[0]['a']
    assert_equal 2, docs[1]['a']
    assert_equal 3, docs[2]['a']
    assert_equal 4, docs[3]['a']

    # Sorting (descending)
    docs = @coll.find({'a' => { '$lt' => 10 }}, :sort => [{'a' => -1}]).map{ |x| x }
    assert_equal 4, docs.size
    assert_equal 4, docs[0]['a']
    assert_equal 3, docs[1]['a']
    assert_equal 2, docs[2]['a']
    assert_equal 1, docs[3]['a']

    # Sorting using array of names; assumes ascending order.
    docs = @coll.find({'a' => { '$lt' => 10 }}, :sort => ['a']).map{ |x| x }
    assert_equal 4, docs.size
    assert_equal 1, docs[0]['a']
    assert_equal 2, docs[1]['a']
    assert_equal 3, docs[2]['a']
    assert_equal 4, docs[3]['a']
    
    docs = @coll.find({'a' => { '$lt' => 10 }}, :sort => ['b', 'a']).map{ |x| x }
    assert_equal 4, docs.size
    assert_equal 2, docs[0]['a']
    assert_equal 4, docs[1]['a']
    assert_equal 1, docs[2]['a']
    assert_equal 3, docs[3]['a']

    # Sorting using empty array; no order guarantee but should not blow up.
    docs = @coll.find({'a' => { '$lt' => 10 }}, :sort => []).map{ |x| x }
    assert_equal 4, docs.size

    # Sorting using ordered hash. You can use an unordered one, but then the
    # order of the keys won't be guaranteed thus your sort won't make sense.
    oh = OrderedHash.new
    oh['a'] = -1
    docs = @coll.find({'a' => { '$lt' => 10 }}, :sort => oh).map{ |x| x }
    assert_equal 4, docs.size
    assert_equal 4, docs[0]['a']
    assert_equal 3, docs[1]['a']
    assert_equal 2, docs[2]['a']
    assert_equal 1, docs[3]['a']

    oh = OrderedHash.new
    oh['b'] = -1
    oh['a'] = 1
    docs = @coll.find({'a' => { '$lt' => 10 }}, :sort => oh).map{ |x| x }
    assert_equal 4, docs.size
    assert_equal 1, docs[0]['a']
    assert_equal 3, docs[1]['a']
    assert_equal 2, docs[2]['a']
    assert_equal 4, docs[3]['a']
  end

  def test_find_limits
    @coll.insert('b' => 2)
    @coll.insert('c' => 3)
    @coll.insert('d' => 4)
  
    docs = @coll.find({}, :limit => 1).map{ |x| x }
    assert_equal 1, docs.size
    docs = @coll.find({}, :limit => 2).map{ |x| x }
    assert_equal 2, docs.size
    docs = @coll.find({}, :limit => 3).map{ |x| x }
    assert_equal 3, docs.size
    docs = @coll.find({}, :limit => 4).map{ |x| x }
    assert_equal 4, docs.size
    docs = @coll.find({}).map{ |x| x }
    assert_equal 4, docs.size
  end

  def test_close
    @db.close
    assert @db.socket.closed?
    begin
      @coll.insert('a' => 1)
      fail "expected IOError exception"
    rescue IOError => ex
      assert_match /closed stream/, ex.to_s
    end
  end

  def test_drop_collection
    assert @db.drop_collection(@coll.name), "drop of collection #{@coll.name} failed"
    assert !@db.collection_names.include?(@coll_full_name)
    @coll = nil
  end

  def test_collection_names
    names = @db.collection_names
    assert names.length >= 1
    assert names.include?(@coll_full_name)

    coll2 = @db.collection('test2')
    coll2.insert('a' => 1)      # collection not created until it's used
    names = @db.collection_names
    assert names.length >= 2
    assert names.include?(@coll_full_name)
    assert names.include?('ruby-mongo-test.test2')
  ensure
    @db.drop_collection('test2')
  end

  def test_collections_info
    cursor = @db.collections_info
    rows = cursor.map{ |x| x }
    assert rows.length >= 1
    row = rows.detect { |r| r['name'] == @coll_full_name }
    assert_not_nil row
    assert_equal @coll.name, row['options']['create']
  end

  def test_collection_options
    @db.drop_collection('foobar')
    @db.strict = true

    begin
      coll = @db.create_collection('foobar', :capped => true, :size => 1024)
      options = coll.options()
      assert_equal 'foobar', options['create']
      assert_equal true, options['capped']
      assert_equal 1024, options['size']
    rescue => ex
      @db.drop_collection('foobar')
      fail "did not expect exception \"#{ex}\""
    end
  end

  def test_full_coll_name
    assert_equal @coll_full_name, @db.full_coll_name(@coll.name)
  end

  def test_index_information
    @db.create_index(@coll.name, 'index_name', ['a'])
    list = @db.index_information(@coll.name)
    assert_equal 1, list.length

    info = list[0]
    assert_equal 'index_name', info[:name]
    assert_equal 1, info[:keys]['a']
  end

  def test_array
    @coll << {'b' => [1, 2, 3]}
    rows = @coll.find({}, {:fields => ['b']}).map{ |x| x }
    assert_equal 1, rows.length
    assert_equal [1, 2, 3], rows[0]['b']
  end

  def test_regex
    regex = /foobar/i
    @coll << {'b' => regex}
    rows = @coll.find({}, {:fields => ['b']}).map{ |x| x }
    assert_equal 1, rows.length
    assert_equal regex, rows[0]['b']
  end

  def test_strict
    assert !@db.strict?
    @db.strict = true
    assert @db.strict?
  end

  def test_strict_access_collection
    @db.strict = true
    begin
      @db.collection('does-not-exist')
      fail "expected exception"
    rescue => ex
      assert_equal "Collection does-not-exist doesn't exist. Currently in strict mode.", ex.to_s
    ensure
      @db.strict = false
      @db.drop_collection('does-not-exist')
    end
  end

  def test_strict_create_collection
    @db.drop_collection('foobar')
    @db.strict = true

    begin
      @db.create_collection('foobar')
      assert true
    rescue => ex
      fail "did not expect exception \"#{ex}\""
    end

    # Now the collection exists. This time we should see an exception.
    begin
      @db.create_collection('foobar')
      fail "expected exception"
    rescue => ex
      assert_equal "Collection foobar already exists. Currently in strict mode.", ex.to_s
    ensure
      @db.strict = false
      @db.drop_collection('foobar')
    end
  end

  def test_ismaster
    assert @db.master?
  end

end
