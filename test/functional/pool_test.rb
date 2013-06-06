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
require 'thread'

class PoolTest < Test::Unit::TestCase
  include Mongo

  def setup
    @client    ||= standard_connection({:pool_size => 15, :pool_timeout => 5})
    @db         = @client.db(MONGO_TEST_DB)
    @collection = @db.collection("pool_test")
  end

  def test_pool_affinity
    pool = Pool.new(@client, TEST_HOST, TEST_PORT, :size => 5)

    threads = []
    10.times do
      threads << Thread.new do
        original_socket = pool.checkout
        pool.checkin(original_socket)
        500.times do
          socket = pool.checkout
          assert_equal original_socket, socket
          pool.checkin(socket)
        end
      end
    end

    threads.each { |t| t.join }
  end

  def test_pool_affinity_max_size
    docs = []
    8000.times {|x| docs << {:value => x}}
    @collection.insert(docs)

    threads = []
    threads << Thread.new do
      @collection.find({"value" => {"$lt" => 100}}).each {|e| e}
      Thread.pass
      sleep(0.125)
      @collection.find({"value" => {"$gt" => 100}}).each {|e| e}
    end
    threads << Thread.new do
      @collection.find({'$where' => "function() {for(i=0;i<1000;i++) {this.value};}"}).each {|e| e}
    end
    threads.each(&:join)
  end
end
