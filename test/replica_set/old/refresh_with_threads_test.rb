$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require './test/replica_sets/rs_test_helper'
require 'benchmark'

class ReplicaSetRefreshWithThreadsTest < Test::Unit::TestCase

  def setup
    ensure_rs
    @client = nil
  end

  def teardown
    @client.close if @conn
  end

  def test_read_write_load_with_added_nodes
    # MongoDB < 2.0 will disconnect clients on rs.reconfig()
    return true if @rs.version < "2"

    seeds = build_seeds(3)
    args = {
      :refresh_interval => 5,
      :refresh_mode => :sync,
      :read => :secondary_preferred
    }
    @client = ReplSetClient.new(seeds, args)
    @duplicate = @client[MONGO_TEST_DB]['duplicate']
    @unique    = @client[MONGO_TEST_DB]['unique']
    @duplicate.insert("test" => "insert")
    @duplicate.insert("test" => "update")
    @unique.insert("test" => "insert")
    @unique.insert("test" => "update")
    @unique.create_index("test", :unique => true)

    threads = []
    10.times do
      threads << Thread.new do
        1000.times do |i|
          if i % 2 == 0
            assert_raise Mongo::OperationFailure do
              @unique.insert({"test" => "insert"}, :safe => true)
            end
          else
            @duplicate.insert({"test" => "insert"}, :safe => true)
          end
        end
      end
    end

    @rs.add_node
    threads.each {|t| t.join }

    sleep(1)
    
    @client['admin'].command({:ismaster => 1})

    assert_equal 3, @client.secondary_pools.length
    assert_equal 3, @client.secondaries.length
  end
end
