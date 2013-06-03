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

class ReplicaSetPinningTest < Test::Unit::TestCase
  def setup
    ensure_cluster(:rs)
    @client = MongoReplicaSetClient.new(@rs.repl_set_seeds, :name => @rs.repl_set_name)
    @db = @client.db(MONGO_TEST_DB)
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
end
