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

class CursorMessageTest < Test::Unit::TestCase

  include Mongo

  @@connection = standard_connection
  @@db   = @@connection.db(MONGO_TEST_DB)
  @@coll = @@db.collection('test')
  @@version = @@connection.server_version

  def setup
    @@coll.remove
    @@coll.insert('a' => 1)     # collection not created until it's used
    @@coll_full_name = "#{MONGO_TEST_DB}.test"
  end

  def test_valid_batch_sizes
    assert_raise ArgumentError do
      @@coll.find({}, :batch_size => 1, :limit => 5)
    end

    assert_raise ArgumentError do
      @@coll.find({}, :batch_size => -1, :limit => 5)
    end

    assert @@coll.find({}, :batch_size => 0, :limit => 5)
  end

  def test_batch_size
    @@coll.remove
    200.times do |n|
      @@coll.insert({:a => n})
    end

    list = @@coll.find({}, :batch_size => 2, :limit => 6).to_a
    assert_equal 6, list.length

    list = @@coll.find({}, :batch_size => 100, :limit => 101).to_a
    assert_equal 101, list.length
  end
end
