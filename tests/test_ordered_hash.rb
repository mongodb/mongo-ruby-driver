$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo/util/ordered_hash'
require 'test/unit'

# NOTE: assumes Mongo is running
class OrderedHashTest < Test::Unit::TestCase

  def setup
    @oh = OrderedHash.new
    @oh['c'] = 1
    @oh['a'] = 2
    @oh['z'] = 3
    @ordered_keys = %w(c a z)
  end

  def test_empty
    assert_equal [], OrderedHash.new.keys
  end

  def test_order_preserved
    assert_equal @ordered_keys, @oh.keys
  end

  def test_order_preserved_after_replace
    @oh['a'] = 42
    assert_equal @ordered_keys, @oh.keys
    @oh['c'] = 'foobar'
    assert_equal @ordered_keys, @oh.keys
    @oh['z'] = /huh?/
    assert_equal @ordered_keys, @oh.keys
  end

  def test_each
    keys = []
    @oh.each { |k, v| keys << k }
    assert_equal keys, @oh.keys

    @oh['z'] = 42
    assert_equal keys, @oh.keys
  end

  def test_values
    assert_equal [1, 2, 3], @oh.values
  end

  def test_merge
    other = OrderedHash.new
    other['f'] = 'foo'
    noob = @oh.merge(other)
    assert_equal @ordered_keys + ['f'], noob.keys
    assert_equal [1, 2, 3, 'foo'], noob.values
  end

  def test_merge_bang
    other = OrderedHash.new
    other['f'] = 'foo'
    @oh.merge!(other)
    assert_equal @ordered_keys + ['f'], @oh.keys
    assert_equal [1, 2, 3, 'foo'], @oh.values
  end

  def test_merge_bang_with_overlap
    other = OrderedHash.new
    other['a'] = 'apple'
    other['c'] = 'crab'
    other['f'] = 'foo'
    @oh.merge!(other)
    assert_equal @ordered_keys + ['f'], @oh.keys
    assert_equal ['crab', 'apple', 3, 'foo'], @oh.values
  end

  def test_merge_bang_with_hash_with_overlap
    other = Hash.new
    other['a'] = 'apple'
    other['c'] = 'crab'
    other['f'] = 'foo'
    @oh.merge!(other)
    assert_equal @ordered_keys + ['f'], @oh.keys
    assert_equal ['crab', 'apple', 3, 'foo'], @oh.values
  end

end
