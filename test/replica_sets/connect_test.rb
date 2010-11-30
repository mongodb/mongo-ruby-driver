$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'mongo'
require 'test/unit'
require './test/test_helper'

# NOTE: This test expects a replica set of three nodes to be running on local host.
class ConnectTest < Test::Unit::TestCase
  include Mongo

  def test_connect_bad_name
    assert_raise_error(ReplicaSetConnectionError, "expected 'wrong-repl-set-name'") do
      Mongo::Connection.multi([['localhost', 27017], ['localhost', 27018], ['localhost', 27019]],
        :rs_name => "wrong-repl-set-name")
    end
  end

  def test_connect
    @conn = Mongo::Connection.multi([['localhost', 27017], ['localhost', 27018], ['localhost', 27019]],
                                    :name => "foo")
    assert @conn.connected?
  end

  def test_connect_with_first_node_down
    puts "Please kill the node at 27017."
    gets

    @conn = Mongo::Connection.multi([['localhost', 27017], ['localhost', 27018], ['localhost', 27019]])
    assert @conn.connected?
  end

  def test_connect_with_second_node_down
    puts "Please kill the node at 27018."
    gets

    @conn = Mongo::Connection.multi([['localhost', 27017], ['localhost', 27018], ['localhost', 27019]])
    assert @conn.connected?
  end

  def test_connect_with_third_node_down
    puts "Please kill the node at 27019."
    gets

    @conn = Mongo::Connection.multi([['localhost', 27017], ['localhost', 27018], ['localhost', 27019]])
    assert @conn.connected?
  end
end
