# encoding:utf-8
require 'test_helper'

class HashWithIndifferentAccessTest < Test::Unit::TestCase
  include BSON

  def setup
    @encoder = BSON::BSON_CODER
  end

  def test_document
    doc = HashWithIndifferentAccess.new
    doc['foo'] = 1
    doc['bar'] = 'baz'

    bson = @encoder.serialize(doc)
    assert_equal doc, @encoder.deserialize(bson.to_s)
  end

  def test_embedded_document
    jimmy = HashWithIndifferentAccess.new
    jimmy['name']     = 'Jimmy'
    jimmy['species'] = 'Siberian Husky'

    stats = HashWithIndifferentAccess.new
    stats['eyes'] = 'blue'

    person = HashWithIndifferentAccess.new
    person['_id'] = BSON::ObjectId.new
    person['name'] = 'Mr. Pet Lover'
    person['pets'] = [jimmy, {'name' => 'Sasha'}]
    person['stats'] = stats

    bson = @encoder.serialize(person)
    assert_equal person, @encoder.deserialize(bson.to_s)
  end

  def test_deserialize_returns_hash_with_indifferent_access
    doc = {:a => 1, 'b' => 2, :c => {:d => 4, 'e' => 5}}
    bson = @encoder.serialize(doc)
    hash = @encoder.deserialize(bson.to_s)
    assert_equal(1, hash['a'])
    assert_equal(1, hash[:a])
    assert_equal(2, hash['b'])
    assert_equal(2, hash[:b])
    assert_equal(4, hash['c']['d'])
    assert_equal(4, hash[:c][:d])
    assert_equal(5, hash['c']['e'])
    assert_equal(5, hash[:c][:e])
  end
end
