$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'mongo'
require 'test/unit'
require './test/test_helper'

# NOTE: This test expects a replica set of three nodes to be running on TEST_HOST,
# on ports TEST_PORT, TEST_PORT + 1, and TEST + 2.
class ConnectTest < Test::Unit::TestCase
  include Mongo

  def test_connect_bad_name
    assert_raise_error(ReplicaSetReplSetConnectionError, "expected 'wrong-repl-set-name'") do
      ReplSetConnection.multi([TEST_HOST, TEST_PORT], [TEST_HOST, TEST_PORT + 1],
        [TEST_HOST, TEST_PORT + 2], :rs_name => "wrong-repl-set-name")
    end
  end

  def test_connect
    @conn = ReplSetConnection.multi([TEST_HOST, TEST_PORT], [TEST_HOST, TEST_PORT + 1],
      [TEST_HOST, TEST_PORT + 2], :name => "foo")
    assert @conn.connected?
  end

  def test_connect_with_first_node_down
    puts "Please kill the node at #{TEST_PORT}."
    gets

    @conn = ReplSetConnection.multi([[TEST_HOST, TEST_PORT], [TEST_HOST, TEST_PORT + 1],
      [TEST_HOST, TEST_PORT + 2]])
    assert @conn.connected?
  end

  def test_connect_with_second_node_down
    puts "Please kill the node at #{TEST_PORT + 1}."
    gets

    @conn = ReplSetConnection.multi([[TEST_HOST, TEST_PORT], [TEST_HOST, TEST_PORT + 1],
      [TEST_HOST, TEST_PORT + 2]])
    assert @conn.connected?
  end

  def test_connect_with_third_node_down
    puts "Please kill the node at #{TEST_PORT + 2}."
    gets

    @conn = ReplSetConnection.multi([[TEST_HOST, TEST_PORT], [TEST_HOST, TEST_PORT + 1],
      [TEST_HOST, TEST_PORT + 2]])
    assert @conn.connected?
  end
end
