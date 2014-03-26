# Copyright (C) 2009-2013 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

  def test_class_regression
    doc = HashWithIndifferentAccess.new
    doc['a'] = 1
    doc['b'] = 2
    doc['c'] = 3

    doc2 = doc.reject { |k, v| v > 1 }
    assert doc2.include?('a')
    assert !doc2.include?('c')
    assert_instance_of HashWithIndifferentAccess, doc2

    doc3 = doc.select { |k, v| v == 2 }
    assert doc3.include?('b')
    assert !doc3.include?('c')
    assert_instance_of HashWithIndifferentAccess, doc3
  end

end
