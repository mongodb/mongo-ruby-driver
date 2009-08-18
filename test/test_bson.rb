$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'
require 'mongo/util/ordered_hash'
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
    doc = {'$where' => Code.new('this.a.b < this.b')}
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

    doc = {"doc" => -5600}
    @b.serialize(doc)
    assert_equal doc, @b.deserialize

    doc = {"doc" => 2147483647}
    @b.serialize(doc)
    assert_equal doc, @b.deserialize

    doc = {"doc" => -2147483648}
    @b.serialize(doc)
    assert_equal doc, @b.deserialize
  end

  def test_ordered_hash
    doc = OrderedHash.new
    doc["b"] = 1
    doc["a"] = 2
    doc["c"] = 3
    doc["d"] = 4
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
    # Mongo only stores up to the millisecond
    assert_in_delta doc['date'], doc2['date'], 0.001
  end

  def test_date_returns_as_utc
    doc = {'date' => Time.now}
    @b.serialize(doc)
    doc2 = @b.deserialize
    assert doc2['date'].utc?
  end

  def test_dbref
    oid = ObjectID.new
    doc = {}
    doc['dbref'] = DBRef.new('namespace', oid)
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
    bin = Binary.new
    'binstring'.each_byte { |b| bin.put(b) }

    doc = {'bin' => bin}
    @b.serialize(doc)
    doc2 = @b.deserialize
    bin2 = doc2['bin']
    assert_kind_of Binary, bin2
    assert_equal 'binstring', bin2.to_s
    assert_equal Binary::SUBTYPE_BYTES, bin2.subtype
  end

  def test_binary_type
    bin = Binary.new([1, 2, 3, 4, 5], Binary::SUBTYPE_USER_DEFINED)

    doc = {'bin' => bin}
    @b.serialize(doc)
    doc2 = @b.deserialize
    bin2 = doc2['bin']
    assert_kind_of Binary, bin2
    assert_equal [1, 2, 3, 4, 5], bin2.to_a
    assert_equal Binary::SUBTYPE_USER_DEFINED, bin2.subtype
  end

  def test_binary_byte_buffer
    bb = ByteBuffer.new
    5.times { |i| bb.put(i + 1) }

    doc = {'bin' => bb}
    @b.serialize(doc)
    doc2 = @b.deserialize
    bin2 = doc2['bin']
    assert_kind_of Binary, bin2
    assert_equal [1, 2, 3, 4, 5], bin2.to_a
    assert_equal Binary::SUBTYPE_BYTES, bin2.subtype
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

  def test_nil_id
    doc = {"_id" => nil}
    assert_equal doc, @b.deserialize(@b.serialize(doc).to_a)
  end

  def test_timestamp
    val = {"test" => [4, 20]}
    assert_equal val, @b.deserialize([0x13, 0x00, 0x00, 0x00,
                                      0x11, 0x74, 0x65, 0x73,
                                      0x74, 0x00, 0x04, 0x00,
                                      0x00, 0x00, 0x14, 0x00,
                                      0x00, 0x00, 0x00])
  end

  def test_overflow
    doc = {"x" => 2**75}
    assert_raise RangeError do
      @b.serialize(doc)
    end

    doc = {"x" => 9223372036854775}
    assert_equal doc, @b.deserialize(@b.serialize(doc).to_a)

    doc = {"x" => 9223372036854775807}
    assert_equal doc, @b.deserialize(@b.serialize(doc).to_a)

    doc["x"] = doc["x"] + 1
    assert_raise RangeError do
      @b.serialize(doc)
    end

    doc = {"x" => -9223372036854775}
    assert_equal doc, @b.deserialize(@b.serialize(doc).to_a)

    doc = {"x" => -9223372036854775808}
    assert_equal doc, @b.deserialize(@b.serialize(doc).to_a)

    doc["x"] = doc["x"] - 1
    assert_raise RangeError do
      @b.serialize(doc)
    end
  end

  def test_do_not_change_original_object
    val = OrderedHash.new
    val['not_id'] = 1
    val['_id'] = 2
    assert val.keys.include?('_id')
    @b.serialize(val)
    assert val.keys.include?('_id')

    val = {'a' => 'foo', 'b' => 'bar', :_id => 42, 'z' => 'hello'}
    assert val.keys.include?(:_id)
    @b.serialize(val)
    assert val.keys.include?(:_id)
  end

end
