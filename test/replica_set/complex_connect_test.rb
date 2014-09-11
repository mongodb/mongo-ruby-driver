# Copyright (C) 2009-2013 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
    authenticate_client(primary)

    @client = MongoReplicaSetClient.new([
      @rs.servers[2].host_port,
      @rs.servers[1].host_port,
      @rs.servers[0].host_port
    ])

    authenticate_client(@client)
    version = @client.server_version

    @client[TEST_DB]['complext-connect-test'].insert({:a => 1})
    assert @client[TEST_DB]['complext-connect-test'].find_one

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
      perform_step_down(primary)
    end

    # isMaster is currently broken in 2.1+ when called on removed nodes
    puts version
    if version < "2.1"
      rescue_connection_failure do
        assert @client[TEST_DB]['complext-connect-test'].find_one
      end

      assert @client[TEST_DB]['complext-connect-test'].find_one
    end

    primary = MongoClient.new(host, @rs.primary.port)
    authenticate_client(primary)
    assert_raise ConnectionFailure do
      primary['admin'].command({:replSetReconfig => old_config})
    end
  end
end
