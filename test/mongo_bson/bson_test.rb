# encoding:utf-8
require 'test/test_helper'
require 'complex'
require 'bigdecimal'
require 'rational'

begin
  require 'active_support/core_ext'
  require 'active_support/hash_with_indifferent_access'
  Time.zone = "Pacific Time (US & Canada)"
  Zone = Time.zone.now
rescue LoadError
  warn 'Could not test BSON with HashWithIndifferentAccess.'
  module ActiveSupport
    class TimeWithZone
    end
  end
  Zone = ActiveSupport::TimeWithZone.new
end

class BSONTest < Test::Unit::TestCase

  include BSON

  def test_read_bson_io_document
    doc = {'doc' => 'hello, world'}
    bson = BSON.serialize(doc)
    io = StringIO.new
    io.write(bson.to_s)
    io.rewind
    assert_equal BSON.deserialize(bson), BSON.read_bson_document(io)
  end

  def test_serialize_returns_byte_buffer
    doc = {'doc' => 'hello, world'}
    bson = BSON.serialize(doc)
    assert bson.is_a?(ByteBuffer)
  end

  def test_deserialize_from_string
    doc = {'doc' => 'hello, world'}
    bson = BSON.serialize(doc)
    string = bson.to_s
    assert_equal doc, BSON.deserialize(string)
  end

  def test_deprecated_bson_module
    doc = {'doc' => 'hello, world'}
    bson = BSON.serialize(doc)
    assert_equal doc, BSON.deserialize(bson)
  end

  def test_string
    doc = {'doc' => 'hello, world'}
    bson = bson = BSON::BSON_CODER.serialize(doc)
    assert_equal doc, BSON::BSON_CODER.deserialize(bson)
  end

  def test_valid_utf8_string
    doc = {'doc' => 'aé'}
    bson = bson = BSON::BSON_CODER.serialize(doc)
    assert_equal doc, BSON::BSON_CODER.deserialize(bson)
  end

  def test_valid_utf8_key
    doc = {'aé' => 'hello'}
    bson = bson = BSON::BSON_CODER.serialize(doc)
    assert_equal doc, BSON::BSON_CODER.deserialize(bson)
  end

  def test_document_length
    doc = {'name' => 'a' * 5 * 1024 * 1024}
    assert_raise InvalidDocument do
      assert BSON::BSON_CODER.serialize(doc)
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
        BSON::BSON_CODER.serialize(doc)
      end
    end

    def test_invalid_key
      key = Iconv.conv('iso-8859-1', 'utf-8', 'aé')
      doc = {key => 'hello'}
      assert_raise InvalidStringEncoding do
        BSON::BSON_CODER.serialize(doc)
      end
    end
  else
    def test_non_utf8_string
      bson = BSON::BSON_CODER.serialize({'str' => 'aé'.encode('iso-8859-1')})
      result = BSON::BSON_CODER.deserialize(bson)['str']
      assert_equal 'aé', result
      assert_equal 'UTF-8', result.encoding.name
    end

    def test_non_utf8_key
      bson = BSON::BSON_CODER.serialize({'aé'.encode('iso-8859-1') => 'hello'})
      assert_equal 'hello', BSON::BSON_CODER.deserialize(bson)['aé']
    end

    # Based on a test from sqlite3-ruby
    def test_default_internal_is_honored
      before_enc = Encoding.default_internal

      str = "壁に耳あり、障子に目あり"
      bson = BSON::BSON_CODER.serialize("x" => str)

      Encoding.default_internal = 'EUC-JP'
      out = BSON::BSON_CODER.deserialize(bson)["x"]

      assert_equal Encoding.default_internal, out.encoding
      assert_equal str.encode('EUC-JP'), out
      assert_equal str, out.encode(str.encoding)
    ensure
      Encoding.default_internal = before_enc
    end
  end

  def test_code
    doc = {'$where' => Code.new('this.a.b < this.b')}
    bson = BSON::BSON_CODER.serialize(doc)
    assert_equal doc, BSON::BSON_CODER.deserialize(bson)
  end

  def test_code_with_scope
    doc = {'$where' => Code.new('this.a.b < this.b', {'foo' => 1})}
    bson = BSON::BSON_CODER.serialize(doc)
    assert_equal doc, BSON::BSON_CODER.deserialize(bson)
  end

  def test_number
    doc = {'doc' => 41.99}
    bson = BSON::BSON_CODER.serialize(doc)
    assert_equal doc, BSON::BSON_CODER.deserialize(bson)
  end

  def test_int
    doc = {'doc' => 42}
    bson = BSON::BSON_CODER.serialize(doc)
    assert_equal doc, BSON::BSON_CODER.deserialize(bson)

    doc = {"doc" => -5600}
    bson = BSON::BSON_CODER.serialize(doc)
    assert_equal doc, BSON::BSON_CODER.deserialize(bson)

    doc = {"doc" => 2147483647}
    bson = BSON::BSON_CODER.serialize(doc)
    assert_equal doc, BSON::BSON_CODER.deserialize(bson)

    doc = {"doc" => -2147483648}
    bson = BSON::BSON_CODER.serialize(doc)
    assert_equal doc, BSON::BSON_CODER.deserialize(bson)
  end

  def test_ordered_hash
    doc = BSON::OrderedHash.new
    doc["b"] = 1
    doc["a"] = 2
    doc["c"] = 3
    doc["d"] = 4
    bson = BSON::BSON_CODER.serialize(doc)
    assert_equal doc, BSON::BSON_CODER.deserialize(bson)
  end

  def test_object
    doc = {'doc' => {'age' => 42, 'name' => 'Spongebob', 'shoe_size' => 9.5}}
    bson = BSON::BSON_CODER.serialize(doc)
    assert_equal doc, BSON::BSON_CODER.deserialize(bson)
  end

  def test_oid
    doc = {'doc' => ObjectID.new}
    bson = BSON::BSON_CODER.serialize(doc)
    assert_equal doc, BSON::BSON_CODER.deserialize(bson)
  end

  def test_array
    doc = {'doc' => [1, 2, 'a', 'b']}
    bson = BSON::BSON_CODER.serialize(doc)
    assert_equal doc, BSON::BSON_CODER.deserialize(bson)
  end

  def test_regex
    doc = {'doc' => /foobar/i}
    bson = BSON::BSON_CODER.serialize(doc)
    doc2 = BSON::BSON_CODER.deserialize(bson)
    assert_equal doc, doc2

    r = doc2['doc']
    assert_kind_of Regexp, r

    doc = {'doc' => r}
    bson_doc = BSON::BSON_CODER.serialize(doc)
    doc2 = nil
    doc2 = BSON::BSON_CODER.deserialize(bson_doc)
    assert_equal doc, doc2
  end

  def test_boolean
    doc = {'doc' => true}
    bson = BSON::BSON_CODER.serialize(doc)
    assert_equal doc, BSON::BSON_CODER.deserialize(bson)
  end

  def test_date
    doc = {'date' => Time.now}
    bson = BSON::BSON_CODER.serialize(doc)
    doc2 = BSON::BSON_CODER.deserialize(bson)
    # Mongo only stores up to the millisecond
    assert_in_delta doc['date'], doc2['date'], 0.001
  end

  def test_date_returns_as_utc
    doc = {'date' => Time.now}
    bson = BSON::BSON_CODER.serialize(doc)
    doc2 = BSON::BSON_CODER.deserialize(bson)
    assert doc2['date'].utc?
  end

  def test_date_before_epoch
    begin
      doc = {'date' => Time.utc(1600)}
      bson = BSON::BSON_CODER.serialize(doc)
      doc2 = BSON::BSON_CODER.deserialize(bson)
      # Mongo only stores up to the millisecond
      assert_in_delta doc['date'], doc2['date'], 0.001
    rescue ArgumentError
      # some versions of Ruby won't let you create pre-epoch Time instances
      #
      # TODO figure out how that will work if somebady has saved data
      # w/ early dates already and is just querying for it.
    end
  end

  def test_exeption_on_using_unsupported_date_class
    [DateTime.now, Date.today, Zone].each do |invalid_date|
      doc = {:date => invalid_date}
      begin
      bson = BSON::BSON_CODER.serialize(doc)
      rescue => e
      ensure
        if !invalid_date.is_a? Time
          assert_equal InvalidDocument, e.class
          assert_match /UTC Time/, e.message
        end
      end
    end
  end

  def test_dbref
    oid = ObjectID.new
    doc = {}
    doc['dbref'] = DBRef.new('namespace', oid)
    bson = BSON::BSON_CODER.serialize(doc)
    doc2 = BSON::BSON_CODER.deserialize(bson)
    assert_equal 'namespace', doc2['dbref'].namespace
    assert_equal oid, doc2['dbref'].object_id
  end

  def test_symbol
    doc = {'sym' => :foo}
    bson = BSON::BSON_CODER.serialize(doc)
    doc2 = BSON::BSON_CODER.deserialize(bson)
    assert_equal :foo, doc2['sym']
  end

  def test_binary
    bin = Binary.new
    'binstring'.each_byte { |b| bin.put(b) }

    doc = {'bin' => bin}
    bson = BSON::BSON_CODER.serialize(doc)
    doc2 = BSON::BSON_CODER.deserialize(bson)
    bin2 = doc2['bin']
    assert_kind_of Binary, bin2
    assert_equal 'binstring', bin2.to_s
    assert_equal Binary::SUBTYPE_BYTES, bin2.subtype
  end

  def test_binary_with_string
    b = Binary.new('somebinarystring')
    doc = {'bin' => b}
    bson = BSON::BSON_CODER.serialize(doc)
    doc2 = BSON::BSON_CODER.deserialize(bson)
    bin2 = doc2['bin']
    assert_kind_of Binary, bin2
    assert_equal 'somebinarystring', bin2.to_s
    assert_equal Binary::SUBTYPE_BYTES, bin2.subtype
  end

  def test_binary_type
    bin = Binary.new([1, 2, 3, 4, 5], Binary::SUBTYPE_USER_DEFINED)

    doc = {'bin' => bin}
    bson = BSON::BSON_CODER.serialize(doc)
    doc2 = BSON::BSON_CODER.deserialize(bson)
    bin2 = doc2['bin']
    assert_kind_of Binary, bin2
    assert_equal [1, 2, 3, 4, 5], bin2.to_a
    assert_equal Binary::SUBTYPE_USER_DEFINED, bin2.subtype
  end

  def test_binary_byte_buffer
    bb = Binary.new
    5.times { |i| bb.put(i + 1) }

    doc = {'bin' => bb}
    bson = BSON::BSON_CODER.serialize(doc)
    doc2 = BSON::BSON_CODER.deserialize(bson)
    bin2 = doc2['bin']
    assert_kind_of Binary, bin2
    assert_equal [1, 2, 3, 4, 5], bin2.to_a
    assert_equal Binary::SUBTYPE_BYTES, bin2.subtype
  end

  def test_put_id_first
    val = BSON::OrderedHash.new
    val['not_id'] = 1
    val['_id'] = 2
    roundtrip = BSON::BSON_CODER.deserialize(BSON::BSON_CODER.serialize(val, false, true).to_a)
    assert_kind_of BSON::OrderedHash, roundtrip
    assert_equal '_id', roundtrip.keys.first

    val = {'a' => 'foo', 'b' => 'bar', :_id => 42, 'z' => 'hello'}
    roundtrip = BSON::BSON_CODER.deserialize(BSON::BSON_CODER.serialize(val, false, true).to_a)
    assert_kind_of BSON::OrderedHash, roundtrip
    assert_equal '_id', roundtrip.keys.first
  end

  def test_nil_id
    doc = {"_id" => nil}
    assert_equal doc, BSON::BSON_CODER.deserialize(bson = BSON::BSON_CODER.serialize(doc, false, true).to_a)
  end

  def test_timestamp
    val = {"test" => [4, 20]}
    assert_equal val, BSON::BSON_CODER.deserialize([0x13, 0x00, 0x00, 0x00,
                                      0x11, 0x74, 0x65, 0x73,
                                      0x74, 0x00, 0x04, 0x00,
                                      0x00, 0x00, 0x14, 0x00,
                                      0x00, 0x00, 0x00])
  end

  def test_overflow
    doc = {"x" => 2**75}
    assert_raise RangeError do
      bson = BSON::BSON_CODER.serialize(doc)
    end

    doc = {"x" => 9223372036854775}
    assert_equal doc, BSON::BSON_CODER.deserialize(BSON::BSON_CODER.serialize(doc).to_a)

    doc = {"x" => 9223372036854775807}
    assert_equal doc, BSON::BSON_CODER.deserialize(BSON::BSON_CODER.serialize(doc).to_a)

    doc["x"] = doc["x"] + 1
    assert_raise RangeError do
      bson = BSON::BSON_CODER.serialize(doc)
    end

    doc = {"x" => -9223372036854775}
    assert_equal doc, BSON::BSON_CODER.deserialize(BSON::BSON_CODER.serialize(doc).to_a)

    doc = {"x" => -9223372036854775808}
    assert_equal doc, BSON::BSON_CODER.deserialize(BSON::BSON_CODER.serialize(doc).to_a)

    doc["x"] = doc["x"] - 1
    assert_raise RangeError do
      bson = BSON::BSON_CODER.serialize(doc)
    end
  end

  def test_invalid_numeric_types
    [BigDecimal.new("1.0"), Complex(0, 1), Rational(2, 3)].each do |type|
      doc = {"x" => type}
      begin
        BSON::BSON_CODER.serialize(doc)
      rescue => e
      ensure
        assert_equal InvalidDocument, e.class
        assert_match /Cannot serialize/, e.message
      end
    end
  end

  def test_do_not_change_original_object
    val = BSON::OrderedHash.new
    val['not_id'] = 1
    val['_id'] = 2
    assert val.keys.include?('_id')
    BSON::BSON_CODER.serialize(val)
    assert val.keys.include?('_id')

    val = {'a' => 'foo', 'b' => 'bar', :_id => 42, 'z' => 'hello'}
    assert val.keys.include?(:_id)
    BSON::BSON_CODER.serialize(val)
    assert val.keys.include?(:_id)
  end

  # note we only test for _id here because in the general case we will
  # write duplicates for :key and "key". _id is a special case because
  # we call has_key? to check for it's existence rather than just iterating
  # over it like we do for the rest of the keys. thus, things like
  # HashWithIndifferentAccess can cause problems for _id but not for other
  # keys. rather than require rails to test with HWIA directly, we do this
  # somewhat hacky test.
  def test_no_duplicate_id
    dup = {"_id" => "foo", :_id => "foo"}
    one = {"_id" => "foo"}

    assert_equal BSON::BSON_CODER.serialize(one).to_a, BSON::BSON_CODER.serialize(dup).to_a
  end

  def test_no_duplicate_id_when_moving_id
    dup = {"_id" => "foo", :_id => "foo"}
    one = {:_id => "foo"}

    assert_equal BSON::BSON_CODER.serialize(one, false, true).to_s, BSON::BSON_CODER.serialize(dup, false, true).to_s
  end

  def test_null_character
    doc = {"a" => "\x00"}

    assert_equal doc, BSON::BSON_CODER.deserialize(BSON::BSON_CODER.serialize(doc).to_a)

    assert_raise InvalidDocument do
      BSON::BSON_CODER.serialize({"\x00" => "a"})
    end

    assert_raise InvalidDocument do
      BSON::BSON_CODER.serialize({"a" => (Regexp.compile "ab\x00c")})
    end
  end

  def test_max_key
    doc = {"a" => MaxKey.new}

    assert_equal doc, BSON::BSON_CODER.deserialize(BSON::BSON_CODER.serialize(doc).to_a)
  end

  def test_min_key
    doc = {"a" => MinKey.new}

    assert_equal doc, BSON::BSON_CODER.deserialize(BSON::BSON_CODER.serialize(doc).to_a)
  end

  def test_invalid_object
    o = Object.new
    assert_raise InvalidDocument do
      BSON::BSON_CODER.serialize({:foo => o})
    end

    assert_raise InvalidDocument do
      BSON::BSON_CODER.serialize({:foo => Date.today})
    end
  end

  def test_move_id
    a = BSON::OrderedHash.new
    a['text'] = 'abc'
    a['key'] = 'abc'
    a['_id']  = 1


    assert_equal ")\000\000\000\020_id\000\001\000\000\000\002text" +
                 "\000\004\000\000\000abc\000\002key\000\004\000\000\000abc\000\000",
                 BSON::BSON_CODER.serialize(a, false, true).to_s
    assert_equal ")\000\000\000\002text\000\004\000\000\000abc\000\002key" +
                 "\000\004\000\000\000abc\000\020_id\000\001\000\000\000\000",
                 BSON::BSON_CODER.serialize(a, false, false).to_s
  end

  def test_move_id_with_nested_doc
    b = BSON::OrderedHash.new
    b['text'] = 'abc'
    b['_id']   = 2
    c = BSON::OrderedHash.new
    c['text'] = 'abc'
    c['hash'] = b
    c['_id']  = 3
    assert_equal ">\000\000\000\020_id\000\003\000\000\000\002text" +
                 "\000\004\000\000\000abc\000\003hash\000\034\000\000" +
                 "\000\002text\000\004\000\000\000abc\000\020_id\000\002\000\000\000\000\000",
                 BSON::BSON_CODER.serialize(c, false, true).to_s
    assert_equal ">\000\000\000\002text\000\004\000\000\000abc\000\003hash" +
                 "\000\034\000\000\000\002text\000\004\000\000\000abc\000\020_id" +
                 "\000\002\000\000\000\000\020_id\000\003\000\000\000\000",
                 BSON::BSON_CODER.serialize(c, false, false).to_s
  end

  if defined?(HashWithIndifferentAccess)
    def test_keep_id_with_hash_with_indifferent_access
      doc = HashWithIndifferentAccess.new
      embedded = HashWithIndifferentAccess.new
      embedded['_id'] = ObjectID.new
      doc['_id']      = ObjectID.new
      doc['embedded'] = [embedded]
      BSON::BSON_CODER.serialize(doc, false, true).to_a
      assert doc.has_key?("_id")
      assert doc['embedded'][0].has_key?("_id")

      doc['_id'] = ObjectID.new
      BSON::BSON_CODER.serialize(doc, false, true).to_a
      assert doc.has_key?("_id")
    end
  end
end
