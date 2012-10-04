require 'test_helper'
require 'pp'

class BasicTest < Test::Unit::TestCase
  def setup
    ensure_cluster(:rs)
  end

  def self.shutdown
    @@cluster.stop
    #@@cluster.clobber
  end

  # TODO member.primary? ==> true
  # To reset after (test) failure
  #     $ killall mongod; rm -fr rs

  def test_connect
    seeds = @rs.repl_set_seeds
    @conn = Mongo::ReplSetConnection.new(seeds, :name => @rs.repl_set_name)
    assert @conn.connected?

    p @rs

    p @conn.primary
    #assert_equal @rs.primary, @conn.primary
    p @conn.secondaries.sort
    #assert_equal @rs.secondaries.sort, @conn.secondaries.sort
    p @conn.arbiters.sort
    #assert_equal @rs.arbiters.sort, @conn.arbiters.sort

    assert_equal(seeds.size, @conn.seeds.size)
    @conn.close
  end

  private

end
