# Copyright (C) 2013 10gen Inc.
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
require 'logger'

class CursorFailTest < Test::Unit::TestCase

  include Mongo

  @@connection = standard_connection
  @@db   = @@connection.db(MONGO_TEST_DB)
  @@coll = @@db.collection('test')
  @@version = @@connection.server_version

  def setup
    @@coll.remove({})
    @@coll.insert({'a' => 1})     # collection not created until it's used
    @@coll_full_name = "#{MONGO_TEST_DB}.test"
  end

  def test_refill_via_get_more_alt_coll
    coll = @@db.collection('test-alt-coll')
    coll.remove
    coll.insert('a' => 1)     # collection not created until it's used
    assert_equal 1, coll.count

    1000.times { |i|
      assert_equal 1 + i, coll.count
      coll.insert('a' => i)
    }

    assert_equal 1001, coll.count
    count = 0
    coll.find.each { |obj|
      count += obj['a']
    }
    assert_equal 1001, coll.count

    # do the same thing again for debugging
    assert_equal 1001, coll.count
    count2 = 0
    coll.find.each { |obj|
      count2 += obj['a']
    }
    assert_equal 1001, coll.count

    assert_equal count, count2
    assert_equal 499501, count
  end

end
