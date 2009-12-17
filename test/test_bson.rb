# encoding:utf-8
require 'test/test_helper'

class BSONTest < Test::Unit::TestCase

  include Mongo

  def test_string
    doc = {'doc' => 'hello, world'}
    bson = bson = BSON.serialize(doc)
    assert_equal doc, BSON.deserialize(bson)
  end

  def test_valid_utf8_string
    doc = {'doc' => 'aé'}
    bson = bson = BSON.serialize(doc)
    assert_equal doc, BSON.deserialize(bson)
  end

  def test_valid_utf8_key
    doc = {'aé' => 'hello'}
    bson = bson = BSON.serialize(doc)
    assert_equal doc, BSON.deserialize(bson)
  end

  def test_document_length
    doc = {'name' => 'a' * 5 * 1024 * 1024}
    assert_raise InvalidDocument do
      assert BSON.serialize(doc)
    end
  end

  # In 1.8 we test that other string encodings raise an exception.
  # In 1.9 we test that they get auto-converted.
  if RUBY_VERSION < '1.9'
    require 'iconv'
    def test_invalid_string
      string = Iconv.conv('iso-8859-1', 'utf-8', 'aé')
      doc = {'doc' => string}
      assert_raise InvalidStringEncoding do
        BSON.serialize(doc)
      end
    end

    def test_invalid_key
      key = Iconv.conv('iso-8859-1', 'utf-8', 'aé')
      doc = {key => 'hello'}
      assert_raise InvalidStringEncoding do
        BSON.serialize(doc)
      end
    end
  else
    def test_non_utf8_string
      bson = BSON.serialize({'str' => 'aé'.encode('iso-8859-1')})
      result = BSON.deserialize(bson)['str']
      assert_equal 'aé', result
      assert_equal 'UTF-8', result.encoding.name
    end

    def test_non_utf8_key
      bson = BSON.serialize({'aé'.encode('iso-8859-1') => 'hello'})
      assert_equal 'hello', BSON.deserialize(bson)['aé']
    end
  end

  def test_code
    doc = {'$where' => Code.new('this.a.b < this.b')}
    bson = BSON.serialize(doc)
    assert_equal doc, BSON.deserialize(bson)
  end

  def test_number
    doc = {'doc' => 41.99}
    bson = BSON.serialize(doc)
    assert_equal doc, BSON.deserialize(bson)
  end

  def test_int
    doc = {'doc' => 42}
    bson = BSON.serialize(doc)
    assert_equal doc, BSON.deserialize(bson)

    doc = {"doc" => -5600}
    bson = BSON.serialize(doc)
    assert_equal doc, BSON.deserialize(bson)

    doc = {"doc" => 2147483647}
    bson = BSON.serialize(doc)
    assert_equal doc, BSON.deserialize(bson)

    doc = {"doc" => -2147483648}
    bson = BSON.serialize(doc)
    assert_equal doc, BSON.deserialize(bson)
  end

  def test_ordered_hash
    doc = OrderedHash.new
    doc["b"] = 1
    doc["a"] = 2
    doc["c"] = 3
    doc["d"] = 4
    bson = BSON.serialize(doc)
    assert_equal doc, BSON.deserialize(bson)
  end

  def test_object
    doc = {'doc' => {'age' => 42, 'name' => 'Spongebob', 'shoe_size' => 9.5}}
    bson = BSON.serialize(doc)
    assert_equal doc, BSON.deserialize(bson)
  end

  def test_oid
    doc = {'doc' => ObjectID.new}
    bson = BSON.serialize(doc)
    assert_equal doc, BSON.deserialize(bson)
  end

  def test_array
    doc = {'doc' => [1, 2, 'a', 'b']}
    bson = BSON.serialize(doc)
    assert_equal doc, BSON.deserialize(bson)
  end

  def test_regex
    doc = {'doc' => /foobar/i}
    bson = BSON.serialize(doc)
    doc2 = BSON.deserialize(bson)
    assert_equal doc, doc2

    r = doc2['doc']
    assert_kind_of RegexpOfHolding, r
    assert_equal '', r.extra_options_str

    r.extra_options_str << 'zywcab'
    assert_equal 'zywcab', r.extra_options_str

    doc = {'doc' => r}
    bson_doc = BSON.serialize(doc)
    doc2 = nil
    doc2 = BSON.deserialize(bson_doc)
    assert_equal doc, doc2

    r = doc2['doc']
    assert_kind_of RegexpOfHolding, r
    assert_equal 'abcwyz', r.extra_options_str # must be sorted
  end

  def test_boolean
    doc = {'doc' => true}
    bson = BSON.serialize(doc)
    assert_equal doc, BSON.deserialize(bson)
  end

  def test_date
    doc = {'date' => Time.now}
    bson = BSON.serialize(doc)
    doc2 = BSON.deserialize(bson)
    # Mongo only stores up to the millisecond
    assert_in_delta doc['date'], doc2['date'], 0.001
  end

  def test_date_returns_as_utc
    doc = {'date' => Time.now}
    bson = BSON.serialize(doc)
    doc2 = BSON.deserialize(bson)
    assert doc2['date'].utc?
  end

  def test_date_before_epoch
    begin
      doc = {'date' => Time.utc(1600)}
      bson = BSON.serialize(doc)
      doc2 = BSON.deserialize(bson)
      # Mongo only stores up to the millisecond
      assert_in_delta doc['date'], doc2['date'], 0.001
    rescue ArgumentError
      # some versions of Ruby won't let you create pre-epoch Time instances
      #
      # TODO figure out how that will work if somebady has saved data
      # w/ early dates already and is just querying for it.
    end
  end

  def test_dbref
    oid = ObjectID.new
    doc = {}
    doc['dbref'] = DBRef.new('namespace', oid)
    bson = BSON.serialize(doc)
    doc2 = BSON.deserialize(bson)
    assert_equal 'namespace', doc2['dbref'].namespace
    assert_equal oid, doc2['dbref'].object_id
  end

  def test_symbol
    doc = {'sym' => :foo}
    bson = BSON.serialize(doc)
    doc2 = BSON.deserialize(bson)
    assert_equal :foo, doc2['sym']
  end

  def test_binary
    bin = Binary.new
    'binstring'.each_byte { |b| bin.put(b) }

    doc = {'bin' => bin}
    bson = BSON.serialize(doc)
    doc2 = BSON.deserialize(bson)
    bin2 = doc2['bin']
    assert_kind_of Binary, bin2
    assert_equal 'binstring', bin2.to_s
    assert_equal Binary::SUBTYPE_BYTES, bin2.subtype
  end

  def test_binary_type
    bin = Binary.new([1, 2, 3, 4, 5], Binary::SUBTYPE_USER_DEFINED)

    doc = {'bin' => bin}
    bson = BSON.serialize(doc)
    doc2 = BSON.deserialize(bson)
    bin2 = doc2['bin']
    assert_kind_of Binary, bin2
    assert_equal [1, 2, 3, 4, 5], bin2.to_a
    assert_equal Binary::SUBTYPE_USER_DEFINED, bin2.subtype
  end

  def test_binary_byte_buffer
    bb = ByteBuffer.new
    5.times { |i| bb.put(i + 1) }

    doc = {'bin' => bb}
    bson = BSON.serialize(doc)
    doc2 = BSON.deserialize(bson)
    bin2 = doc2['bin']
    assert_kind_of Binary, bin2
    assert_equal [1, 2, 3, 4, 5], bin2.to_a
    assert_equal Binary::SUBTYPE_BYTES, bin2.subtype
  end

  def test_put_id_first
    val = OrderedHash.new
    val['not_id'] = 1
    val['_id'] = 2
    roundtrip = BSON.deserialize(BSON.serialize(val).to_a)
    assert_kind_of OrderedHash, roundtrip
    assert_equal '_id', roundtrip.keys.first

    val = {'a' => 'foo', 'b' => 'bar', :_id => 42, 'z' => 'hello'}
    roundtrip = BSON.deserialize(BSON.serialize(val).to_a)
    assert_kind_of OrderedHash, roundtrip
    assert_equal '_id', roundtrip.keys.first
  end

  def test_nil_id
    doc = {"_id" => nil}
    assert_equal doc, BSON.deserialize(bson = BSON.serialize(doc).to_a)
  end

  def test_timestamp
    val = {"test" => [4, 20]}
    assert_equal val, BSON.deserialize([0x13, 0x00, 0x00, 0x00,
                                      0x11, 0x74, 0x65, 0x73,
                                      0x74, 0x00, 0x04, 0x00,
                                      0x00, 0x00, 0x14, 0x00,
                                      0x00, 0x00, 0x00])
  end

  def test_overflow
    doc = {"x" => 2**75}
    assert_raise RangeError do
      bson = BSON.serialize(doc)
    end

    doc = {"x" => 9223372036854775}
    assert_equal doc, BSON.deserialize(BSON.serialize(doc).to_a)

    doc = {"x" => 9223372036854775807}
    assert_equal doc, BSON.deserialize(BSON.serialize(doc).to_a)

    doc["x"] = doc["x"] + 1
    assert_raise RangeError do
      bson = BSON.serialize(doc)
    end

    doc = {"x" => -9223372036854775}
    assert_equal doc, BSON.deserialize(BSON.serialize(doc).to_a)

    doc = {"x" => -9223372036854775808}
    assert_equal doc, BSON.deserialize(BSON.serialize(doc).to_a)

    doc["x"] = doc["x"] - 1
    assert_raise RangeError do
      bson = BSON.serialize(doc)
    end
  end

  def test_do_not_change_original_object
    val = OrderedHash.new
    val['not_id'] = 1
    val['_id'] = 2
    assert val.keys.include?('_id')
    BSON.serialize(val)
    assert val.keys.include?('_id')

    val = {'a' => 'foo', 'b' => 'bar', :_id => 42, 'z' => 'hello'}
    assert val.keys.include?(:_id)
    BSON.serialize(val)
    assert val.keys.include?(:_id)
  end

  # note we only test for _id here because in the general case we will
  # write duplicates for :key and "key". _id is a special case because
  # we call has_key? to check for it's existance rather than just iterating
  # over it like we do for the rest of the keys. thus, things like
  # HashWithIndifferentAccess can cause problems for _id but not for other
  # keys. rather than require rails to test with HWIA directly, we do this
  # somewhat hacky test.
  def test_no_duplicate_id
    dup = {"_id" => "foo", :_id => "foo"}
    one = {"_id" => "foo"}

    assert_equal BSON.serialize(one).to_a, BSON.serialize(dup).to_a
  end

  def test_null_character
    doc = {"a" => "\x00"}

    assert_equal doc, BSON.deserialize(BSON.serialize(doc).to_a)

    assert_raise InvalidDocument do
      BSON.serialize({"\x00" => "a"})
    end

    assert_raise InvalidDocument do
      BSON.serialize({"a" => (Regexp.compile "ab\x00c")})
    end
  end

end
