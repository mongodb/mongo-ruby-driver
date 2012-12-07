require 'test_helper'

class ComplexConnectTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:rs)
  end

  def teardown
    @client.close if defined?(@conn) && @conn
  end

  def test_complex_connect
    host = @rs.servers.first.host
    primary = MongoClient.new(host, @rs.primary.port)

    @client = MongoReplicaSetClient.new([
      @rs.servers[2].host_port,
      @rs.servers[1].host_port,
      @rs.servers[0].host_port
    ])

    version = @client.server_version

    @client['test']['foo'].insert({:a => 1})
    assert @client['test']['foo'].find_one

    config = primary['local']['system.replset'].find_one
    old_config = config.dup
    config['version'] += 1

    # eliminate exception: can't find self in new replset config
    port_to_delete = @rs.servers.collect(&:port).find{|port| port != primary.port}.to_s

    config['members'].delete_if do |member|
      member['host'].include?(port_to_delete)
    end

    assert_raise ConnectionFailure do
      primary['admin'].command({:replSetReconfig => config})
    end
    @rs.start


    assert_raise ConnectionFailure do
      primary['admin'].command(step_down_command)
    end

    # isMaster is currently broken in 2.1+ when called on removed nodes
    puts version
    if version < "2.1"
      rescue_connection_failure do
        assert @client['test']['foo'].find_one
      end

      assert @client['test']['foo'].find_one
    end

    primary = MongoClient.new(host, @rs.primary.port)
    assert_raise ConnectionFailure do
      primary['admin'].command({:replSetReconfig => old_config})
    end
  end
end
