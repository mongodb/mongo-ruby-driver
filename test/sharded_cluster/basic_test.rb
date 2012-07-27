$:.unshift(File.expand_path('../..', File.dirname(__FILE__)))
require 'test/sharded_cluster/sc_test_helper'
require 'test/tools/mongo_config'

class BasicTest < Test::Unit::TestCase

  def self.suite
    s = super
    def s.setup

    end
    def s.teardown
      @@sc.stop
      @@sc.clobber
    end
    def s.run(*args)
      setup
      super
      teardown
    end
    s
  end

  def setup
    ensure_sc
  end

  def teardown

  end

  # TODO member.primary? ==> true
  def test_connect
    seeds = @sc.mongos_seeds
    @con = Mongo::ShardedConnection.new(seeds)
    assert @con.connected?
    assert_equal(seeds.size, @con.seeds.size)
    probe(seeds.size)
    @con.close
  end

  def test_hard_refresh
    seeds = @sc.mongos_seeds
    @con = Mongo::ShardedConnection.new(seeds)
    assert @con.connected?
    @con.hard_refresh!
    assert @con.connected?
    @con.close
  end

  def test_reconnect
    seeds = @sc.mongos_seeds
    @con = Mongo::ShardedConnection.new(seeds)
    assert @con.connected?
    router = @sc.servers(:routers).first
    router.stop
    probe(seeds.size)
    assert @con.connected?
    @con.close
  end

  def test_all_down
    seeds = @sc.mongos_seeds
    @con = Mongo::ShardedConnection.new(seeds)
    assert @con.connected?
    @sc.servers(:routers).each{|router| router.stop}
    assert_raises Mongo::ConnectionFailure do
      probe(seeds.size)
    end
    assert_false @con.connected?
    @con.close
  end

  def test_cycle
    seeds = @sc.mongos_seeds
    @con = Mongo::ShardedConnection.new(seeds)
    assert @con.connected?
    routers = @sc.servers(:routers)
    while routers.size > 0 do
      rescue_connection_failure do
        probe(seeds.size)
      end
      probe(seeds.size)
      #p @con.manager.primary
      router = routers.detect{|r| r.port == @con.manager.primary.last}
      routers.delete(router)
      router.stop
    end
    assert_raises Mongo::ConnectionFailure do
      probe(seeds.size)
    end
    assert_false @con.connected?
    routers = @sc.servers(:routers).reverse
    routers.each do |r|
      r.start
      @con.hard_refresh!
      #p @con.manager.primary
      rescue_connection_failure do
        probe(seeds.size)
      end
      probe(seeds.size)
    end
    @con.close
  end

  private

  def probe(size)
    assert_equal(size, @con['config']['mongos'].find.to_a.size)
  end

end
