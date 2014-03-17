# Copyright (C) 2009-2013 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License")
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

class BulkApiStressTest < Test::Unit::TestCase

  # Generate a large string of 'size' MB (estimated
  # by a string of 'size' * 1024 * 1024 characters).
  def generate_large_string(size)
    s = "a" * (size * 1024 * 1024)
  end

  def setup
    @client = standard_connection
    @db = @client[TEST_DB]
    @coll = @db["bulk-api-stress-tests"]
    @coll.remove
  end

  def test_ordered_batch_large_inserts
    bulk = @coll.initialize_ordered_bulk_op
    s = generate_large_string(4)

    for i in 0..5
      bulk.insert({:_id => i, :msg => s})
    end
    bulk.insert({:_id => 3}) # error
    bulk.insert({:_id => 100})

    ex = assert_raise BulkWriteError do
      bulk.execute
    end

    error_details = ex.result
    assert_equal 6, error_details["nInserted"]
    assert_equal 1, error_details["writeErrors"].length
    error = error_details["writeErrors"][0]
    assert_equal 11000, error["code"] # duplicate key error
    assert error["errmsg"].kind_of? String
    assert_equal 6, error["index"]
    assert_equal 6, @coll.count()
  end

  def test_unordered_batch_large_inserts
    bulk = @coll.initialize_unordered_bulk_op
    s = generate_large_string(4)

    for i in 0..5
      bulk.insert({:_id => i, :msg => s})
    end
    bulk.insert({:_id => 3}) # error
    bulk.insert({:_id => 100})

    ex = assert_raise BulkWriteError do
      bulk.execute
    end

    error_details = ex.result
    assert_equal 7, error_details["nInserted"]
    assert_equal 1, error_details["writeErrors"].length
    error = error_details["writeErrors"][0]
    assert_equal 11000, error["code"] # duplicate key error
    assert error["errmsg"].kind_of? String
    assert_equal 6, error["index"]
    assert_equal 7, @coll.count()
  end

  def test_large_single_insert
    bulk = @coll.initialize_unordered_bulk_op
    s = generate_large_string(17)
    bulk.insert({:a => s})
# RUBY-730:
#    ex = assert_raise BulkWriteError do
#      bulk.execute
#    end
  end

  def test_ordered_batch_large_batch
    bulk = @coll.initialize_ordered_bulk_op

    bulk.insert({:_id => 1600})
    for i in 0..2000
      bulk.insert({:_id => i})
    end

    ex = assert_raise BulkWriteError do
      bulk.execute
    end

    error_details = ex.result
    assert_equal 1601, error_details["nInserted"]
    assert_equal 1, error_details["writeErrors"].length
    error = error_details["writeErrors"][0]
    assert_equal 11000, error["code"] # duplicate key error
    assert error["errmsg"].kind_of? String
    assert_equal 1601, error["index"]
    assert_equal 1601, @coll.count()
  end

  def test_unordered_batch_large_batch
    bulk = @coll.initialize_unordered_bulk_op

    bulk.insert({:_id => 1600})
    for i in 0..2000
      bulk.insert({:_id => i})
    end

    ex = assert_raise BulkWriteError do
      bulk.execute
    end

    error_details = ex.result
    assert_equal 2001, error_details["nInserted"]
    assert_equal 1, error_details["writeErrors"].length
    error = error_details["writeErrors"][0]
    assert_equal 11000, error["code"] # duplicate key error
    assert error["errmsg"].kind_of? String
    assert_equal 1601, error["index"]
    assert_equal 2001, @coll.count()
  end
end
