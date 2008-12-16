$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'
require 'test/unit'

# NOTE: assumes Mongo is running
class BSONTest < Test::Unit::TestCase

  def setup
    @b = BSON.new
  end

  def test_object_encoding
    doc = {'doc' => {'age' => 42, 'name' => 'Spongebob', 'shoe_size' => 9.5}}
    @b.serialize(doc)
    assert_equal doc, @b.deserialize
  end

  def test_code_encoding
    doc = {'$where' => 'this.a.b < this.b'}
    @b.serialize(doc)
    assert_equal doc, @b.deserialize
  end
end
