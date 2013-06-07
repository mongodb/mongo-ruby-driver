# Copyright (C) 2013 10gen Inc.
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

class NodeTest < Test::Unit::TestCase

  def setup
    @client = stub()
    manager = mock('pool_manager')
    manager.stubs(:update_max_sizes)
    @client.stubs(:local_manager).returns(manager)
  end

  should "refuse to connect to node without 'hosts' key" do
    tcp = mock()
    node = Node.new(@client, ['localhost', 27017])
    tcp.stubs(:new).returns(new_mock_socket)
    @client.stubs(:socket_class).returns(tcp)

    admin_db = new_mock_db
    admin_db.stubs(:command).returns({'ok' => 1, 'ismaster' => 1})
    @client.stubs(:[]).with('admin').returns(admin_db)
    @client.stubs(:op_timeout).returns(nil)
    @client.stubs(:connect_timeout).returns(nil)
    @client.expects(:log)
    @client.expects(:mongos?).returns(false)
    @client.stubs(:socket_opts)

    assert node.connect
    node.config
  end

  should "load a node from an array" do
    node = Node.new(@client, ['power.level.com', 9001])
    assert_equal 'power.level.com', node.host
    assert_equal 9001, node.port
    assert_equal 'power.level.com:9001', node.address
  end

  should "should default the port for an array" do
    node = Node.new(@client, ['power.level.com'])
    assert_equal 'power.level.com', node.host
    assert_equal MongoClient::DEFAULT_PORT, node.port
    assert_equal "power.level.com:#{MongoClient::DEFAULT_PORT}", node.address
  end

  should "load a node from a string" do
    node = Node.new(@client, 'localhost:1234')
    assert_equal 'localhost', node.host
    assert_equal 1234, node.port
    assert_equal 'localhost:1234', node.address
  end

  should "should default the port for a string" do
    node = Node.new(@client, '192.168.0.1')
    assert_equal '192.168.0.1', node.host
    assert_equal MongoClient::DEFAULT_PORT, node.port
    assert_equal "192.168.0.1:#{MongoClient::DEFAULT_PORT}", node.address
  end

  should "two nodes with the same address should be equal" do
    assert_equal Node.new(@client, '192.168.0.1'),
      Node.new(@client, ['192.168.0.1', MongoClient::DEFAULT_PORT])
  end

  should "two nodes with the same address should have the same hash" do
    assert_equal Node.new(@client, '192.168.0.1').hash,
      Node.new(@client, ['192.168.0.1', MongoClient::DEFAULT_PORT]).hash
  end

  should "two nodes with different addresses should not be equal" do
    assert_not_equal Node.new(@client, '192.168.0.2'),
      Node.new(@client, ['192.168.0.1', MongoClient::DEFAULT_PORT])
  end

  should "two nodes with the same address should have the same hash negate" do
    assert_not_equal Node.new(@client, '192.168.0.1').hash,
      Node.new(@client, '1239.33.4.2393:29949').hash
  end

end
