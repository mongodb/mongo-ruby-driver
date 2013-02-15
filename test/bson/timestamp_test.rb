require 'test_helper'

class TimestampTest < Test::Unit::TestCase

  def test_timestamp_to_s
    t1 = Timestamp.new(5000, 200)
    assert_equal "seconds: 5000, increment: 200", t1.to_s
  end

  def test_timestamp_equality
    t1 = Timestamp.new(5000, 200)
    t2 = Timestamp.new(5000, 200)
    assert_equal t2, t1
  end

  def test_timestamp_range
    t = 1;
    while(t < 1_000_000_000 ) do
      ts = Timestamp.new(t, 0)
      doc = {:ts => ts}
      bson = BSON::BSON_CODER.serialize(doc)
      assert_equal doc[:ts], BSON::BSON_CODER.deserialize(bson)['ts']
      t = t * 10
    end
  end

  def test_timestamp_32bit_compatibility
    max_32bit_fixnum = (1 << 30) - 1
    test_val = max_32bit_fixnum + 10

    ts = Timestamp.new(test_val, test_val)
    doc = {:ts => ts}
    bson = BSON::BSON_CODER.serialize(doc)
    assert_equal doc[:ts], BSON::BSON_CODER.deserialize(bson)['ts']
  end

  def test_implements_array_for_backward_compatibility
    silently do
      ts = Timestamp.new(5000, 200)
      assert_equal 200, ts[0]
      assert_equal 5000, ts[1]

      array = ts.map {|t| t }
      assert_equal 2, array.length

      assert_equal 200, array[0]
      assert_equal 5000, array[1]
    end
  end

end
