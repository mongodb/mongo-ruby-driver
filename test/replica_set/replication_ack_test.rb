require 'test_helper'

class ReplicaSetAckTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:rs)
    @client = MongoReplicaSetClient.new(@rs.repl_set_seeds)

    @slave1 = MongoClient.new(
      @client.secondary_pools.first.host,
      @client.secondary_pools.first.port, :slave_ok => true)

    assert !@slave1.read_primary?

    @db = @client.db(MONGO_TEST_DB)
    @db.drop_collection("test-sets")
    @col = @db.collection("test-sets")
  end

  def teardown
    @client.close if @conn
  end

  def test_safe_mode_with_w_failure
    assert_raise_error OperationFailure, "timeout" do
      @col.insert({:foo => 1}, :w => 4, :wtimeout => 1, :fsync => true)
    end
    assert_raise_error OperationFailure, "timeout" do
      @col.update({:foo => 1}, {:foo => 2}, :w => 4, :wtimeout => 1, :fsync => true)
    end
    assert_raise_error OperationFailure, "timeout" do
      @col.remove({:foo => 2}, :w => 4, :wtimeout => 1, :fsync => true)
    end
    assert_raise_error OperationFailure do
      @col.insert({:foo => 3}, :w => "test-tag")
    end
  end

  def test_safe_mode_replication_ack
    @col.insert({:baz => "bar"}, :w => 3, :wtimeout => 5000)

    assert @col.insert({:foo => "0" * 5000}, :w => 3, :wtimeout => 5000)
    assert_equal 2, @slave1[MONGO_TEST_DB]["test-sets"].count

    assert @col.update({:baz => "bar"}, {:baz => "foo"}, :w => 3, :wtimeout => 5000)
    assert @slave1[MONGO_TEST_DB]["test-sets"].find_one({:baz => "foo"})

    assert @col.insert({:foo => "bar"}, :w => "majority")

    assert @col.insert({:bar => "baz"}, :w => :majority)

    assert @col.remove({}, :w => 3, :wtimeout => 5000)
    assert_equal 0, @slave1[MONGO_TEST_DB]["test-sets"].count
  end

  def test_last_error_responses
    20.times { @col.insert({:baz => "bar"}) }
    response = @db.get_last_error(:w => 3, :wtimeout => 5000)
    assert response['ok'] == 1
    assert response['lastOp']

    @col.update({}, {:baz => "foo"})
    response = @db.get_last_error(:w => 3, :wtimeout => 5000)
    assert response['ok'] == 1
    assert response['lastOp']

    @col.remove({})
    response =  @db.get_last_error(:w => 3, :wtimeout => 5000)
    assert response['ok'] == 1
    assert response['n'] == 20
    assert response['lastOp']
  end

end
