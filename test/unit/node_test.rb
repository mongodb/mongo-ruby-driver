require './test/test_helper'

class NodeTest < Test::Unit::TestCase

  def setup
    @connection = stub()
  end

  should "refuse to connect to node without 'hosts' key" do
    tcp = mock()
    node = Node.new(@connection, ['localhost', 27017])
    tcp.stubs(:new).returns(new_mock_socket)
    @connection.stubs(:socket_class).returns(tcp)

    admin_db = new_mock_db
    admin_db.stubs(:command).returns({'ok' => 1, 'ismaster' => 1})
    @connection.stubs(:[]).with('admin').returns(admin_db)
    @connection.stubs(:connect_timeout).returns(nil)
    @connection.expects(:log)

    assert node.connect
    node.set_config
  end

  should "load a node from an array" do
    node = Node.new(@connection, ['power.level.com', 9001])
    assert_equal 'power.level.com', node.host
    assert_equal 9001, node.port
    assert_equal 'power.level.com:9001', node.address
  end

  should "should default the port for an array" do
    node = Node.new(@connection, ['power.level.com'])
    assert_equal 'power.level.com', node.host
    assert_equal Connection::DEFAULT_PORT, node.port
    assert_equal "power.level.com:#{Connection::DEFAULT_PORT}", node.address
  end

  should "load a node from a string" do
    node = Node.new(@connection, 'localhost:1234')
    assert_equal 'localhost', node.host
    assert_equal 1234, node.port
    assert_equal 'localhost:1234', node.address
  end

  should "should default the port for a string" do
    node = Node.new(@connection, '192.168.0.1')
    assert_equal '192.168.0.1', node.host
    assert_equal Connection::DEFAULT_PORT, node.port
    assert_equal "192.168.0.1:#{Connection::DEFAULT_PORT}", node.address
  end

  should "two nodes with the same address should be equal" do
    assert_equal Node.new(@connection, '192.168.0.1'),
      Node.new(@connection, ['192.168.0.1', Connection::DEFAULT_PORT])
  end

  should "two nodes with the same address should have the same hash" do
    assert_equal Node.new(@connection, '192.168.0.1').hash,
      Node.new(@connection, ['192.168.0.1', Connection::DEFAULT_PORT]).hash
  end

  should "two nodes with different addresses should not be equal" do
    assert_not_equal Node.new(@connection, '192.168.0.2'),
      Node.new(@connection, ['192.168.0.1', Connection::DEFAULT_PORT])
  end

  should "two nodes with the same address should have the same hash negate" do
    assert_not_equal Node.new(@connection, '192.168.0.1').hash,
      Node.new(@connection, '1239.33.4.2393:29949').hash
  end

end
