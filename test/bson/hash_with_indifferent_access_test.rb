# encoding:utf-8
require 'test_helper'

# Note: HashWithIndifferentAccess is so commonly used
# that we always need to make sure that the driver works
# with it. However, the bson gem should never need to
# depend on it.

# As a result, ActiveSupport is no longer a gem dependency and it should remain
# that way. It must be required by the application code or
# via bundler for developmet.

require 'bson/support/hash_with_indifferent_access'

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
end
