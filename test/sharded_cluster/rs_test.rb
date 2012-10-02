$:.unshift(File.expand_path('../..', File.dirname(__FILE__)))
require 'test/sharded_cluster/rs_test_helper'
require 'test/tools/mongo_config'

class BasicTest < Test::Unit::TestCase

  def self.suite
    s = super
    def s.setup

    end
    def s.teardown
      @@rs.stop
      @@rs.clobber
    end
    def s.run(*args)
      setup
      super
      teardown
    end
    s
  end

  def setup
    ensure_rs
  end


  def teardown

  end

  # TODO member.primary? ==> true
  # To reset after (test) failure
  #     $ killall mongod; rm -fr rs

  def test_connect
    seeds = @rs.replica_seeds
    @conn = Mongo::ReplSetConnection.new(seeds, :name => @rs.name)
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
