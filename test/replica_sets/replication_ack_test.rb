$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'mongo'
require 'test/unit'
require './test/test_helper'

# NOTE: This test expects a replica set of three nodes to be running on local host.
class ReplicaSetAckTest < Test::Unit::TestCase
  include Mongo

  def setup
    @conn = Mongo::Connection.multi([[TEST_HOST, TEST_PORT], [TEST_HOST, TEST_PORT + 1], [TEST_HOST, TEST_PORT + 2]])

    master = [@conn.primary_pool.host, @conn.primary_pool.port]

    @slave1 = Mongo::Connection.new(@conn.secondary_pools[0].host, @conn.secondary_pools[0].port, :slave_ok => true)

    @db = @conn.db(MONGO_TEST_DB)
    @db.drop_collection("test-sets")
    @col = @db.collection("test-sets")
  end

  def test_safe_mode_with_w_failure
    assert_raise_error OperationFailure, "timeout" do
      @col.insert({:foo => 1}, :safe => {:w => 4, :wtimeout => 1, :fsync => true})
    end
    assert_raise_error OperationFailure, "timeout" do
      @col.update({:foo => 1}, {:foo => 2}, :safe => {:w => 4, :wtimeout => 1, :fsync => true})
    end
    assert_raise_error OperationFailure, "timeout" do
      @col.remove({:foo => 2}, :safe => {:w => 4, :wtimeout => 1, :fsync => true})
    end
  end

  def test_safe_mode_replication_ack
    @col.insert({:baz => "bar"}, :safe => {:w => 2, :wtimeout => 1000})

    assert @col.insert({:foo => "0" * 10000}, :safe => {:w => 2, :wtimeout => 1000})
    assert_equal 2, @slave1[MONGO_TEST_DB]["test-sets"].count


    assert @col.update({:baz => "bar"}, {:baz => "foo"}, :safe => {:w => 2, :wtimeout => 1000})
    assert @slave1[MONGO_TEST_DB]["test-sets"].find_one({:baz => "foo"})

    assert @col.remove({}, :safe => {:w => 2, :wtimeout => 1000})
    assert_equal 0, @slave1[MONGO_TEST_DB]["test-sets"].count
  end

  def test_last_error_responses
    20.times { @col.insert({:baz => "bar"}) }
    response = @db.get_last_error(:w => 2, :wtimeout => 10000)
    assert response['ok'] == 1
    assert response['lastOp']

    @col.update({}, {:baz => "foo"}, :multi => true)
    response = @db.get_last_error(:w => 2, :wtimeout => 1000)
    assert response['ok'] == 1
    assert response['lastOp']

    @col.remove({})
    response =  @db.get_last_error(:w => 2, :wtimeout => 1000)
    assert response['ok'] == 1
    assert response['n'] == 20
    assert response['lastOp']
  end

end
