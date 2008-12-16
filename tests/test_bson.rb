$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'
require 'test/unit'

# NOTE: assumes Mongo is running
class BSONTest < Test::Unit::TestCase

  def setup
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
    doc = {'doc' => XGen::Mongo::Driver::ObjectID.new}
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
    assert_equal doc, @b.deserialize
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

  def test_null
  end
end
