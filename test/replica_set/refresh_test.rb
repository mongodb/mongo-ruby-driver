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

class ReplicaSetRefreshTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:rs)
  end

  def test_connect_and_manual_refresh_with_secondary_down
    num_secondaries = @rs.secondaries.size
    client = MongoReplicaSetClient.new(@rs.repl_set_seeds, :refresh_mode => false)
    authenticate_client(client)

    assert_equal num_secondaries, client.secondaries.size
    assert client.connected?
    assert_equal client.read_pool, client.primary_pool
    old_refresh_version = client.refresh_version

    @rs.stop_secondary

    client.refresh
    assert_equal num_secondaries - 1, client.secondaries.size
    assert client.connected?
    assert_equal client.read_pool, client.primary_pool
    assert client.refresh_version > old_refresh_version
    old_refresh_version = client.refresh_version

    # Test no changes after restart until manual refresh
    @rs.restart
    assert_equal num_secondaries - 1, client.secondaries.size
    assert client.connected?
    assert_equal client.read_pool, client.primary_pool
    assert_equal client.refresh_version, old_refresh_version

    # Refresh and ensure state
    client.refresh
    assert_equal num_secondaries, client.secondaries.size
    assert client.connected?
    assert_equal client.read_pool, client.primary_pool
    assert client.refresh_version > old_refresh_version
  end

  def test_automated_refresh_with_secondary_down
    num_secondaries = @rs.secondaries.size
    client = MongoReplicaSetClient.new(@rs.repl_set_seeds,
      :refresh_interval => 1, :refresh_mode => :sync, :read => :secondary_preferred)
    authenticate_client(client)

    # Ensure secondaries are all recognized by client and client is connected
    assert_equal num_secondaries, client.secondaries.size
    assert client.connected?
    assert client.secondary_pools.include?(client.read_pool)
    pool = client.read_pool

    @rs.member_by_name(pool.host_string).stop
    sleep(2)

    old_refresh_version = client.refresh_version
    # Trigger synchronous refresh
    client[TEST_DB]['rs-refresh-test'].find_one

    assert client.connected?
    assert client.refresh_version > old_refresh_version
    assert_equal num_secondaries - 1, client.secondaries.size
    assert client.secondary_pools.include?(client.read_pool)
    assert_not_equal pool, client.read_pool

    # Restart nodes and ensure refresh interval has passed
    @rs.restart
    sleep(2)

    old_refresh_version = client.refresh_version
    # Trigger synchronous refresh
    client[TEST_DB]['rs-refresh-test'].find_one

    assert client.connected?
    assert client.refresh_version > old_refresh_version,
      "Refresh version hasn't changed."
    assert_equal num_secondaries, client.secondaries.size
      "No secondaries have been added."
    assert_equal num_secondaries, client.secondary_pools.size
  end

  def test_concurrent_refreshes
    factor = 5
    nthreads = factor * 10
    threads = []
    client = MongoReplicaSetClient.new(@rs.repl_set_seeds, :refresh_mode => :sync, :refresh_interval => 1)
    authenticate_client(client)

    nthreads.times do |i|
      threads << Thread.new do
        # force a connection failure every couple of threads that causes a refresh
        if i % factor == 0
          cursor = client[TEST_DB]['rs-refresh-test'].find
          cursor.stubs(:checkout_socket_from_connection).raises(ConnectionFailure)
          begin
            cursor.next
          rescue => ex
            raise ex unless ex.class == ConnectionFailure
            next
          end
        else
          # synchronous refreshes will happen every couple of find_ones
          cursor = client[TEST_DB]['rs-refresh-test'].find_one
        end
      end
    end

    threads.each do |t|
      t.join
    end
  end

  def test_manager_recursive_locking
  # See RUBY-775
  # This tests that there isn't recursive locking when a pool manager reconnects
  # to all replica set members. The bug in RUBY-775 occurred because the same lock
  # acquired in order to connect the pool manager was used to read the pool manager's
  # state.
    client = MongoReplicaSetClient.from_uri(@uri)

    cursor = client[TEST_DB]['rs-refresh-test'].find
    client.stubs(:receive_message).raises(ConnectionFailure)
    client.manager.stubs(:refresh_required?).returns(true)
    client.manager.stubs(:check_connection_health).returns(true)
    assert_raise ConnectionFailure do
       cursor.next
    end
  end

=begin
  def test_automated_refresh_with_removed_node
    client = MongoReplicaSetClient.new(@rs.repl_set_seeds,
      :refresh_interval => 1, :refresh_mode => :sync)
    authenticate_client(client)

    num_secondaries = client.secondary_pools.length
    old_refresh_version = client.refresh_version

    n = @rs.repl_set_remove_node(2)
    sleep(2)

    rescue_connection_failure do
      client[TEST_DB]['rs-refresh-test'].find_one
    end

    assert client.refresh_version > old_refresh_version,
      "Refresh version hasn't changed."
    assert_equal num_secondaries - 1, client.secondaries.length
    assert_equal num_secondaries - 1, client.secondary_pools.length

    #@rs.add_node(n)
  end

  def test_adding_and_removing_nodes
    client = MongoReplicaSetClient.new(build_seeds(3),
      :refresh_interval => 2, :refresh_mode => :sync)

    @rs.add_node
    sleep(4)
    client[TEST_DB]['rs-refresh-test'].find_one

    @conn2 = MongoReplicaSetClient.new(build_seeds(3),
      :refresh_interval => 2, :refresh_mode => :sync)

    assert @conn2.secondaries.sort == client.secondaries.sort,
      "Second connection secondaries not equal to first."
    assert_equal 3, client.secondary_pools.length
    assert_equal 3, client.secondaries.length

    config = client['admin'].command({:ismaster => 1})

    @rs.remove_secondary_node
    sleep(4)
    config = client['admin'].command({:ismaster => 1})

    assert_equal 2, client.secondary_pools.length
    assert_equal 2, client.secondaries.length
  end
=end
end