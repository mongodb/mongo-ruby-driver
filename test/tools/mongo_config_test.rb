require 'test_helper'

class MongoConfig < Test::Unit::TestCase

  def startup
    @sys_proc = nil
  end

  def shutdown
    @sys_proc.stop if @sys_proc && @sys_proc.running?
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
    @sys_proc = Mongo::Config::SysProc.new(cmd)
    assert_equal(cmd, @sys_proc.cmd)
    assert_nil(@sys_proc.pid)
    start_and_assert_running?(@sys_proc)
  end

  test "SysProc wait" do
    @sys_proc = Mongo::Config::SysProc.new("true")
    start_and_assert_running?(@sys_proc)
    assert(@sys_proc.running?)
    @sys_proc.wait
    assert(!@sys_proc.running?)
  end

  test "SysProc kill" do
    @sys_proc = Mongo::Config::SysProc.new("true")
    start_and_assert_running?(@sys_proc)
    @sys_proc.kill
    @sys_proc.wait
    assert(!@sys_proc.running?)
  end

  test "SysProc stop" do
    @sys_proc = Mongo::Config::SysProc.new("true")
    start_and_assert_running?(@sys_proc)
    @sys_proc.stop
    assert(!@sys_proc.running?)
  end

  test "SysProc zombie respawn" do
    @sys_proc = Mongo::Config::SysProc.new("true")
    start_and_assert_running?(@sys_proc)
    prev_pid = @sys_proc.pid
    @sys_proc.kill
    # don't wait, leaving a zombie
    assert(@sys_proc.running?)
    start_and_assert_running?(@sys_proc)
    assert(prev_pid && @sys_proc.pid && prev_pid != @sys_proc.pid, 'SysProc#start should spawn a new process after a zombie')
    @sys_proc.stop
    assert(!@sys_proc.running?)
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
    assert_equal("mongod --dbpath data --logpath data/log", server.cmd)
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
    yield manager
    manager.stop
    manager.servers.each{|s| assert(!s.running?)}
    manager.clobber
  end

  test "cluster manager base" do
    cluster_test(Mongo::Config::DEFAULT_BASE_OPTS) do |manager|

    end
  end

  test "cluster manager replica set" do
    cluster_test(Mongo::Config::DEFAULT_REPLICA_SET) do |manager|
      servers = manager.servers
      servers.each do |server|    
        assert_not_nil(Mongo::MongoClient.new(server.host, server.port))
        assert_match(/oplogSize/, server.cmd, '--oplogSize option should be specified')
        assert_match(/smallfiles/, server.cmd, '--smallfiles option should be specified')
        assert_no_match(/nojournal/, server.cmd, '--nojournal option should not be specified')
        assert_match(/noprealloc/, server.cmd, '--noprealloc option should be specified')
      end
    end
  end

  test "cluster manager sharded simple" do
    cluster_test(Mongo::Config::DEFAULT_SHARDED_SIMPLE) do |manager|
      servers = manager.shards + manager.configs
      servers.each do |server|
        assert_not_nil(Mongo::MongoClient.new(server.host, server.port))
        assert_match(/oplogSize/, server.cmd, '--oplogSize option should be specified')
        assert_match(/smallfiles/, server.cmd, '--smallfiles option should be specified')
        assert_no_match(/nojournal/, server.cmd, '--nojournal option should not be specified')
        assert_match(/noprealloc/, server.cmd, '--noprealloc option should be specified')
      end
    end
  end

  test "cluster manager sharded replica" do
    #cluster_test(Mongo::Config::DEFAULT_SHARDED_REPLICA) # not yet supported by ClusterManager
  end

  private

  def start_and_assert_running?(sys_proc)
    assert_not_nil(sys_proc.start(0))
    assert_not_nil(sys_proc.pid)
    assert(sys_proc.running?)
  end

end

