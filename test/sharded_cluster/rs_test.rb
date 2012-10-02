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
  def test_connect
    seeds = @rs.mongos_seeds
    @con = Mongo::ShardedConnection.new(seeds)
    assert @con.connected?
    assert_equal(seeds.size, @con.seeds.size)
    probe(seeds.size)
    @con.close
  end

  private

  def probe(size)
    assert_equal(size, @con['config']['mongos'].find.to_a.size)
  end

end
