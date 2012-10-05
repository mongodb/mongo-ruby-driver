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

    conn = Mongo::ReplSetConnection.new(*@rs.repl_set_seeds_old, :name => @rs.repl_set_name)
    assert conn.connected?
    conn.close
  end

  private

end
