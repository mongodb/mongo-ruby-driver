$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require './test/replica_sets/rs_test_helper'
require 'benchmark'

class ReplicaSetRefreshWithThreadsTest < Test::Unit::TestCase
  include ReplicaSetTest

  def setup
    @conn = nil
  end

  def teardown
    self.rs.restart_killed_nodes
    @conn.close if @conn
  end

  def test_read_write_load_with_added_nodes
    @conn = ReplSetConnection.new([self.rs.host, self.rs.ports[0]],
                                  [self.rs.host, self.rs.ports[1]],
                                  [self.rs.host, self.rs.ports[2]],
                                  :refresh_interval => 5,
                                  :refresh_mode => :sync,
                                  :read => :secondary)
    @duplicate = @conn[MONGO_TEST_DB]['duplicate']
    @unique    = @conn[MONGO_TEST_DB]['unique']
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

    self.rs.add_node
    threads.each {|t| t.join }

    config = @conn['admin'].command({:ismaster => 1})

    assert_equal 3, @conn.secondary_pools.length
    assert_equal 3, @conn.secondaries.length
  end
end
