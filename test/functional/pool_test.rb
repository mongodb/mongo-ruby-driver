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

class PoolTest < Test::Unit::TestCase
  include Mongo

  def setup
    @client    ||= standard_connection({:pool_size => 15, :pool_timeout => 5})
    @db         = @client.db(TEST_DB)
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

  def test_auth_network_error
    # Make sure there's no semaphore leak if we get a network error
    # when authenticating a new socket with cached credentials.

    # Get a client with one socket so we detect if it's leaked.
    client = MongoClient.new(TEST_HOST, TEST_PORT, :pool_size => 1, :pool_timeout => 1)
    assert_equal 1, client.pool_size

    # Set up the client with a pool
    client[TEST_DB].command(:ping => 1)

    # Close the one socket in the pool
    pool = client.primary_pool
    socket = pool.instance_variable_get(:@sockets).first
    socket.close

    # Simulate an authenticate() call on a different socket.
    # Cache the creds on the client.
    creds = {
        :db_name   => TEST_DB,
        :username  => TEST_USER,
        :password  => TEST_USER_PWD,
        :source    => TEST_DB,
        :mechanism => Mongo::Authentication::DEFAULT_MECHANISM,
        :extra     => {}
    }
    client.auths << creds

    # The client authenticates its socket with the
    # new credential, but gets a socket.error.
    client[TEST_DB]['ruby-test'].find_one

    # # No semaphore leak, the pool is allowed to make a new socket.
    assert_equal 1, pool.instance_variable_get(:@sockets).size
  end

  def test_socket_cleanup
    # Get a client with one socket so we detect if it's leaked.
    client = MongoClient.new(TEST_HOST, TEST_PORT, :pool_size => 1, :pool_timeout => 1)
    assert_equal 1, client.pool_size

    # Set up the client with a pool
    client[TEST_DB].command(:ping => 1)

    # Simulate an authenticate() call on a different socket.
    # Cache the creds on the client.
    creds = {
        :db_name   => TEST_DB,
        :username  => TEST_USER,
        :password  => TEST_USER_PWD,
        :source    => TEST_DB,
        :mechanism => Mongo::Authentication::DEFAULT_MECHANISM,
        :extra     => {}
    }
    client.auths << creds

    # Mock the socket to raise a ConnectionFailure when applying auths
    pool   = client.primary_pool
    socket = pool.instance_variable_get(:@sockets).first
    socket.expects(:send).raises(ConnectionFailure)

    # Checkout a socket from the pool to force it to get a new socket
    pool.checkout
    new_socket = pool.instance_variable_get(:@sockets).first

    # Make sure the pool is cleaned up properly
    assert_not_equal socket, new_socket
    assert_equal 1, pool.instance_variable_get(:@sockets).size
    assert_equal 1, pool.instance_variable_get(:@thread_ids_to_sockets).size
    assert !pool.instance_variable_get(:@checked_out).include?(socket)
    assert !pool.instance_variable_get(:@sockets).include?(socket)
  end
end
