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

class ReplicaSetPinningTest < Test::Unit::TestCase
  def setup
    ensure_cluster(:rs)
    @client = MongoReplicaSetClient.from_uri(@uri, :op_timeout => TEST_OP_TIMEOUT)
    @db = @client.db(TEST_DB)
    @coll = @db.collection("test-sets")
    @coll.insert({:a => 1})
  end

  def test_unpinning
    # pin primary
    @coll.find_one
    assert_equal @client.pinned_pool[:pool], @client.primary_pool

    # pin secondary
    @coll.find_one({}, :read => :secondary_preferred)
    assert @client.secondary_pools.include? @client.pinned_pool[:pool]

    # repin primary
    @coll.find_one({}, :read => :primary_preferred)
    assert_equal @client.pinned_pool[:pool], @client.primary_pool
  end

  def test_pinned_pool_is_local_to_thread
    threads = []
    30.times do |i|
      threads << Thread.new do
        if i % 2 == 0
          @coll.find_one({}, :read => :secondary_preferred)
          assert @client.secondary_pools.include? @client.pinned_pool[:pool]
        else
          @coll.find_one({}, :read => :primary_preferred)
          assert_equal @client.pinned_pool[:pool], @client.primary_pool
        end
      end
    end
    threads.each(&:join)
  end

  def test_aggregation_cursor_pinning
    return unless @client.server_version >= '2.5.1'
    @coll.drop

    [10, 1000].each do |size|
      @coll.drop
      size.times {|i| @coll.insert({ :_id => i }) }
      expected_sum = size.times.reduce(:+)

      cursor = @coll.aggregate(
          [{ :$project => {:_id => '$_id'}} ],
          :cursor => { :batchSize => 1 }
      )

      assert_equal Mongo::Cursor, cursor.class

      cursor_sum = cursor.reduce(0) do |sum, doc|
        sum += doc['_id']
      end

      assert_equal expected_sum, cursor_sum
    end
    @coll.drop
  end

  def test_parallel_scan_pinning
    return unless @client.server_version >= '2.5.5'
    @coll.drop

    8000.times { |i| @coll.insert({ :_id => i }) }

    lock = Mutex.new
    doc_ids = Set.new
    threads = []
    cursors = @coll.parallel_scan(3)
    cursors.each_with_index do |cursor, i|
      threads << Thread.new do
        docs = cursor.to_a
        lock.synchronize do
          docs.each do |doc|
            doc_ids << doc['_id']
          end
        end
      end
    end
    threads.each(&:join)
    assert_equal 8000, doc_ids.count
    @coll.drop
  end
end
