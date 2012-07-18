$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require './test/replica_sets/rs_test_helper'

class ComplexConnectTest < Test::Unit::TestCase
  
  def setup
    ensure_rs
  end

  def teardown
    @rs.restart_killed_nodes
    @conn.close if defined?(@conn) && @conn
  end

  def test_complex_connect
    primary = Connection.new(@rs.host, @rs.ports[0])

    @conn = ReplSetConnection.new([
      "#{@rs.host}:#{@rs.ports[2]}",
      "#{@rs.host}:#{@rs.ports[1]}",
      "#{@rs.host}:#{@rs.ports[0]}",
    ])

    version = @conn.server_version

    @conn['test']['foo'].insert({:a => 1})
    assert @conn['test']['foo'].find_one

    config = primary['local']['system.replset'].find_one
    config['version'] += 1
    config['members'].delete_if do |member|
      member['host'].include?(@rs.ports[2].to_s)
    end

    assert_raise ConnectionFailure do
      primary['admin'].command({:replSetReconfig => config})
    end
    @rs.ensure_up

    force_stepdown = BSON::OrderedHash.new
    force_stepdown[:replSetStepDown] = 1
    force_stepdown[:force] = true

    assert_raise ConnectionFailure do
      primary['admin'].command(force_stepdown)
    end

    # isMaster is currently broken in 2.1+ when called on removed nodes
    if version < "2.1"
      rescue_connection_failure do
        assert @conn['test']['foo'].find_one
      end

      assert @conn['test']['foo'].find_one
    end
  end
end
