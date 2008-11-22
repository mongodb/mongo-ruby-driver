$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'
require 'test/unit'

class ByteBufferTest < Test::Unit::TestCase

  def setup
    @buf = ByteBuffer.new
  end

  def test_empty
    assert_equal 0, @buf.length
  end

  def test_length
    @buf.put_int 3
    assert_equal 4, @buf.length
  end

  def test_default_order
    assert_equal :little_endian, @buf.order
  end

  def test_long_length
    @buf.put_long 1027
    assert_equal 8, @buf.length
  end

  def test_get_long
    @buf.put_long 1027
    @buf.rewind
    assert_equal 1027, @buf.get_long
  end

  def test_rewrite
    @buf.put_int(0)
    @buf.rewind
    @buf.put_int(1027)
    assert_equal 4, @buf.length
    @buf.rewind
    assert_equal 1027, @buf.get_int
    assert_equal 4, @buf.position
  end

end
