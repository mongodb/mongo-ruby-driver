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

class ReplicaSetAckTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:rs)
    @client = MongoReplicaSetClient.new(@rs.repl_set_seeds)

    @slave1 = MongoClient.new(
      @client.secondary_pools.first.host,
      @client.secondary_pools.first.port, :slave_ok => true)

    assert !@slave1.read_primary?

    @db = @client.db(TEST_DB)
    @db.drop_collection("test-sets")
    @col = @db.collection("test-sets")
  end

  def teardown
    @client.close if @conn
  end

  def test_safe_mode_with_w_failure
    assert_raise_error WriteConcernError, "time" do
      @col.insert({:foo => 1}, :w => 4, :wtimeout => 1, :fsync => true)
    end
    assert_raise_error WriteConcernError, "time" do
      @col.update({:foo => 1}, {:foo => 2}, :w => 4, :wtimeout => 1, :fsync => true)
    end
    assert_raise_error WriteConcernError, "time" do
      @col.remove({:foo => 2}, :w => 4, :wtimeout => 1, :fsync => true)
    end
    if @client.server_version >= '2.5.4'
      assert_raise_error WriteConcernError do
        @col.insert({:foo => 3}, :w => "test-tag")
      end
    else # indistinguishable "errmsg"=>"exception: unrecognized getLastError mode: test-tag"
      assert_raise_error OperationFailure do
        @col.insert({:foo => 3}, :w => "test-tag")
      end
    end
  end

  def test_safe_mode_replication_ack
    @col.insert({:baz => "bar"}, :w => 3, :wtimeout => 5000)

    assert @col.insert({:foo => "0" * 5000}, :w => 3, :wtimeout => 5000)
    assert_equal 2, @slave1[TEST_DB]["test-sets"].count

    assert @col.update({:baz => "bar"}, {:baz => "foo"}, :w => 3, :wtimeout => 5000)
    assert @slave1[TEST_DB]["test-sets"].find_one({:baz => "foo"})

    assert @col.insert({:foo => "bar"}, :w => "majority")

    assert @col.insert({:bar => "baz"}, :w => :majority)

    assert @col.remove({}, :w => 3, :wtimeout => 5000)
    assert_equal 0, @slave1[TEST_DB]["test-sets"].count
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
