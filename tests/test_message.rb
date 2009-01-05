$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'
require 'test/unit'

class MessageTest < Test::Unit::TestCase

  include XGen::Mongo::Driver

  def setup
    @msg = Message.new(42)
  end

  def test_initial_info
    assert_equal Message::HEADER_SIZE, @msg.buf.length
    @msg.write_long(1029)
    @msg.buf.rewind
    assert_equal Message::HEADER_SIZE + 8, @msg.buf.get_int
    @msg.buf.get_int            # skip message id
    assert_equal 0, @msg.buf.get_int
    assert_equal 42,  @msg.buf.get_int
    assert_equal 1029, @msg.buf.get_long
  end

  def test_update_length
    @msg.update_message_length
    @msg.buf.rewind
    assert_equal Message::HEADER_SIZE, @msg.buf.get_int
  end

  def test_long_length
    @msg.write_long(1027)
    assert_equal Message::HEADER_SIZE + 8, @msg.buf.length
  end

end
