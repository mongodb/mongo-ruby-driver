$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'
require 'test/unit'

class ObjectIDTest < Test::Unit::TestCase

  include XGen::Mongo::Driver

  def setup
    @t = 42
    @o = ObjectID.new(nil, @t)
  end

  def test_index_for_time
    t = 99
    assert_equal 0, @o.index_for_time(t)
    assert_equal 1, @o.index_for_time(t)
    assert_equal 2, @o.index_for_time(t)
    t = 100
    assert_equal 0, @o.index_for_time(t)
  end

  def test_time_bytes
    a = @o.to_a
    assert_equal @t, a[0]
    3.times { |i| assert_equal 0, a[i+1] }

    t = 43
    o = ObjectID.new(nil, t)
    a = o.to_a
    assert_equal t, a[0]
    3.times { |i| assert_equal 0, a[i+1] }
    assert_equal 1, o.index_for_time(t) # 0 was used for o
  end

  def test_different
    o2 = ObjectID.new(nil, @t)
    assert @o.to_a != o2.to_a
  end

  def test_eql?
    o2 = ObjectID.new(@o.to_a)
    assert @o.eql?(o2)
    assert @o == o2
  end

  def test_to_s
    s = @o.to_s
    assert_equal 24, s.length
    s =~ /^([0-9a-f]+)$/
    assert_equal 24, $1.length
  end

  def test_save_and_restore
    host = ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost'
    port = ENV['MONGO_RUBY_DRIVER_PORT'] || Mongo::DEFAULT_PORT
    db = Mongo.new(host, port).db('ruby-mongo-test')
    coll = db.collection('test')

    coll.clear
    coll << {'a' => 1, '_id' => @o}

    row = coll.find().collect.first
    assert_equal 1, row['a']
    assert_equal @o, row['_id']
  end

  def test_from_string
    hex_str = @o.to_s
    o2 = ObjectID.from_string(hex_str)
    assert_equal hex_str, o2.to_s
    assert_equal @o, o2
    assert_equal @o.to_s, o2.to_s
  end

  def test_legal
    assert !ObjectID.legal?(nil)
    assert !ObjectID.legal?("fred")
    assert !ObjectID.legal?("0000")
    assert !ObjectID.legal?('000102030405060708090A0')
    assert ObjectID.legal?('000102030405060708090A0B')
    assert ObjectID.legal?('abcdefABCDEF123456789012')
    assert !ObjectID.legal?('abcdefABCDEF12345678901x')
  end

  def test_from_string_leading_zeroes
    hex_str = '000000000000000000abcdef'
    o = ObjectID.from_string(hex_str)
    assert_equal hex_str, o.to_s
  end

  def test_byte_order
    hex_str = '000102030405060708090A0B'
    o = ObjectID.from_string(hex_str)
    assert_equal [0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x00, 0x0b, 0x0a, 0x09, 0x08], o.to_a
  end

end
