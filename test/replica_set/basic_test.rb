require 'test_helper'
require 'pp'

class BasicTest < Test::Unit::TestCase
  def setup
    ensure_cluster(:rs)
  end

  def self.shutdown
    @@cluster.stop
    @@cluster.clobber
  end

  # TODO member.primary? ==> true
  # To reset after (test) failure
  #     $ killall mongod; rm -fr rs

  def test_connect
    conn = Mongo::ReplSetConnection.new(@rs.repl_set_seeds, :name => @rs.repl_set_name)
    assert conn.connected?
    assert_equal @rs.primary, conn.primary.join(':')
    assert_equal @rs.secondaries.sort, conn.secondaries.collect{|s| s.join(':')}.sort
    assert_equal @rs.arbiters.sort, conn.arbiters.collect{|s| s.join(':')}.sort
    conn.close

    conn = Mongo::ReplSetConnection.new(@rs.repl_set_seeds_old, :name => @rs.repl_set_name)
    assert conn.connected?
    conn.close
  end

  def test_multiple_concurrent_replica_set_connection
    conn1 = ReplSetConnection.new(@rs.repl_set_seeds, :name => @rs.repl_set_name)
    conn2 = ReplSetConnection.new(@rs.repl_set_seeds, :name => @rs.repl_set_name)
    assert conn1.connected?
    assert conn2.connected?

    assert conn1.manager != conn2.manager
    assert conn1.local_manager != conn2.local_manager
  end

  #def test_cache_original_seed_nodes
  #  seeds = @rs.repl_set_seeds << "#{@rs.host}:19356"
  #  conn = ReplSetConnection.new(seeds, :name => @rs.repl_set_name)
  #  assert conn.connected?
  #  assert conn.seeds.include?([@rs.host, 19356]), "Original seed nodes not cached!"
  #  assert_equal [@rs.host, 19356], conn.seeds.last, "Original seed nodes not cached!"
  #end

end
