$:.unshift(File.expand_path('../../lib', File.dirname(__FILE__))).unshift(File.expand_path('../..', File.dirname(__FILE__)))
require 'test-unit'
require 'test/tools/mongo_config'

class MongoConfig < Test::Unit::TestCase

  def self.suite
    s = super
    def s.setup

    end
    def s.teardown

    end
    def s.run(*args)
      setup
      super
      teardown
    end
    s
  end

  test "config defaults" do
    [ Mongo::Config::DEFAULT_BASE_OPTS,
      Mongo::Config::DEFAULT_REPLICA_SET,
      Mongo::Config::DEFAULT_SHARDED_SIMPLE,
      Mongo::Config::DEFAULT_SHARDED_REPLICA
    ].each do |params|
      config = Mongo::Config.cluster(params)
      assert(config.size > 0)
    end
  end

  test "get available port" do
    assert_not_nil(Mongo::Config.get_available_port)
  end

  test "SysProc start" do
    cmd = "true"
    sys_proc = Mongo::Config::SysProc.new(cmd)
    assert_equal(cmd, sys_proc.cmd)
    assert_nil(sys_proc.pid)
    assert_not_nil(sys_proc.start(0))
    assert_not_nil(sys_proc.pid)
  end

  test "SysProc wait" do
    sys_proc = Mongo::Config::SysProc.new("true")
    assert_not_nil(sys_proc.start(0))
    assert(sys_proc.running?)
    sys_proc.wait
    assert(!sys_proc.running?)
  end

  test "SysProc kill" do
    sys_proc = Mongo::Config::SysProc.new("true")
    assert_not_nil(sys_proc.start(0))
    sys_proc.kill
    sys_proc.wait
    assert(!sys_proc.running?)
  end

  test "SysProc stop" do
    sys_proc = Mongo::Config::SysProc.new("true")
    assert_not_nil(sys_proc.start(0))
    sys_proc.stop
    assert(!sys_proc.running?)
  end

  test "Server" do
    server = Mongo::Config::Server.new('a cmd', 'host', 1234)
    assert_equal('a cmd', server.cmd)
    assert_equal('host', server.host)
    assert_equal(1234, server.port)
  end

  test "DbServer" do
    config = Mongo::Config::DEFAULT_BASE_OPTS
    server = Mongo::Config::DbServer.new(config)
    assert_equal(config, server.config)
    assert_equal("mongod --logpath data/log --dbpath data", server.cmd)
    assert_equal(config[:host], server.host)
    assert_equal(config[:port], server.port)
  end

  def cluster_test(opts)
    #debug 1, opts.inspect
    config =  Mongo::Config.cluster(opts)
    #debug 1, config.inspect
    manager = Mongo::Config::ClusterManager.new(config)
    assert_equal(config, manager.config)
    manager.start
    manager.servers.each{|s| p s}
    manager.stop
    manager.servers.each{|s| assert_equal(false, s.running?)}
    manager.clobber
  end

  test "cluster manager base" do
    #cluster_test(Mongo::Config::DEFAULT_BASE_OPTS)
  end

  test "cluster manager replica set" do
    #cluster_test(Mongo::Config::DEFAULT_REPLICA_SET)
  end

  test "cluster manager sharded simple" do
    #manager = Mongo::Config::ClusterManager.new(Mongo::Config.cluster(Mongo::Config::DEFAULT_SHARDED_SIMPLE)).start
    opts = Mongo::Config::DEFAULT_SHARDED_SIMPLE
    #debug 1, opts.inspect
    config =  Mongo::Config.cluster(opts)
    #debug 1, config.inspect
    manager = Mongo::Config::ClusterManager.new(config)
    assert_equal(config, manager.config)
    manager.start
    #debug 1, manager.ismaster
    #debug 1, manager.mongos_discover
    manager.stop.clobber
  end

  test "cluster manager sharded replica" do
    #cluster_test(Mongo::Config::DEFAULT_SHARDED_REPLICA)
  end

end

