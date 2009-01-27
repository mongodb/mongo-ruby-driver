$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'
require 'test/unit'

class BSONTest < Test::Unit::TestCase

  include XGen::Mongo::Driver

  def setup
    # We don't pass a DB to the constructor, even though we are about to test
    # deserialization. This means that when we deserialize, any DBRefs will
    # have nil @db ivars. That's fine for now.
    @b = BSON.new
  end

  def test_string
    doc = {'doc' => 'hello, world'}
    @b.serialize(doc)
    assert_equal doc, @b.deserialize
  end

  def test_code
    doc = {'$where' => 'this.a.b < this.b'}
    @b.serialize(doc)
    assert_equal doc, @b.deserialize
  end

  def test_number
    doc = {'doc' => 41.99}
    @b.serialize(doc)
    assert_equal doc, @b.deserialize
  end

  def test_int
    doc = {'doc' => 42}
    @b.serialize(doc)
    assert_equal doc, @b.deserialize
  end

  def test_object
    doc = {'doc' => {'age' => 42, 'name' => 'Spongebob', 'shoe_size' => 9.5}}
    @b.serialize(doc)
    assert_equal doc, @b.deserialize
  end

  def test_oid
    doc = {'doc' => ObjectID.new}
    @b.serialize(doc)
    assert_equal doc, @b.deserialize
  end

  def test_array
    doc = {'doc' => [1, 2, 'a', 'b']}
    @b.serialize(doc)
    assert_equal doc, @b.deserialize
  end

  def test_regex
    doc = {'doc' => /foobar/i}
    @b.serialize(doc)
    doc2 = @b.deserialize
    assert_equal doc, doc2

    r = doc2['doc']
    assert_kind_of XGen::Mongo::Driver::RegexpOfHolding, r
    assert_equal '', r.extra_options_str

    r.extra_options_str << 'zywcab'
    assert_equal 'zywcab', r.extra_options_str

    b = BSON.new
    doc = {'doc' => r}
    b.serialize(doc)
    doc2 = nil
    doc2 = b.deserialize
    assert_equal doc, doc2

    r = doc2['doc']
    assert_kind_of XGen::Mongo::Driver::RegexpOfHolding, r
    assert_equal 'abcwyz', r.extra_options_str # must be sorted
  end

  def test_boolean
    doc = {'doc' => true}
    @b.serialize(doc)
    assert_equal doc, @b.deserialize
  end

  def test_date
    doc = {'date' => Time.now}
    @b.serialize(doc)
    doc2 = @b.deserialize
    # Mongo only stores seconds, so comparing raw Time objects will fail
    # because the fractional seconds will be different.
    assert_equal doc['date'].to_i, doc2['date'].to_i
  end

  def test_dbref
    oid = ObjectID.new
    doc = {}
    doc['dbref'] = DBRef.new(doc, 'dbref', nil, 'namespace', oid)
    @b.serialize(doc)
    doc2 = @b.deserialize
    assert_equal 'namespace', doc2['dbref'].namespace
    assert_equal oid, doc2['dbref'].object_id
  end

  def test_symbol
    doc = {'sym' => :foo}
    @b.serialize(doc)
    doc2 = @b.deserialize
    assert_equal :foo, doc2['sym']
  end

  def test_binary
    bin = 'binstring'.to_mongo_binary
    assert_kind_of Binary, bin

    doc = {'bin' => bin}
    @b.serialize(doc)
    doc2 = @b.deserialize
    assert_equal 'binstring', doc2['bin']
  end

  def test_binary_byte_buffer
    bb = ByteBuffer.new
    10.times { |i| bb.put(i) }
    doc = {'bin' => bb}
    @b.serialize(doc)
    doc2 = @b.deserialize

    doc2_bytes = []
    doc2['bin'].each_byte { |b| doc2_bytes << b }
    assert_equal bb.to_a, doc2_bytes
  end

  def test_undefined
    doc = {'undef' => Undefined.new}
    @b.serialize(doc)
    doc2 = @b.deserialize
    assert_kind_of Undefined, doc2['undef']
  end

  def test_put_id_first
    val = OrderedHash.new
    val['not_id'] = 1
    val['_id'] = 2
    roundtrip = @b.deserialize(@b.serialize(val).to_a)
    assert_kind_of OrderedHash, roundtrip
    assert_equal '_id', roundtrip.keys.first

    val = {'a' => 'foo', 'b' => 'bar', :_id => 42, 'z' => 'hello'}
    roundtrip = @b.deserialize(@b.serialize(val).to_a)
    assert_kind_of OrderedHash, roundtrip
    assert_equal '_id', roundtrip.keys.first
  end

end
