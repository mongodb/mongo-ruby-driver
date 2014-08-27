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

class MaxValuesTest < Test::Unit::TestCase

  include Mongo

  def setup
    ensure_cluster(:rs)
    @client = MongoReplicaSetClient.from_uri(@uri)
    @db = new_mock_db
    @client.stubs(:[]).returns(@db)
    @ismaster = {
      'hosts' => @client.local_manager.hosts.to_a,
      'arbiters' => @client.local_manager.arbiters
    }
  end

  def test_initial_max_and_min_values
    assert @client.max_bson_size
    assert @client.max_message_size
    assert @client.max_wire_version
    assert @client.min_wire_version
  end

  def test_updated_max_and_min_sizes_after_node_config_change
    @db.stubs(:command).returns(
      @ismaster.merge({'ismaster' => true}),
      @ismaster.merge({'secondary' => true, 'maxMessageSizeBytes' => 1024 * MESSAGE_SIZE_FACTOR}),
      @ismaster.merge({'secondary' => true, 'maxBsonObjectSize' => 1024}),
      @ismaster.merge({'secondary' => true, 'maxWireVersion' => 0}),
      @ismaster.merge({'secondary' => true, 'minWireVersion' => 0})
    )
    @client.local_manager.stubs(:refresh_required?).returns(true)
    @client.refresh

    assert_equal 1024, @client.max_bson_size
    assert_equal 1024 * MESSAGE_SIZE_FACTOR, @client.max_message_size
    assert_equal 0, @client.max_wire_version
    assert_equal 0, @client.min_wire_version
  end

  def test_no_values_in_config
    @db.stubs(:command).returns(
      @ismaster.merge({'ismaster' => true}),
      @ismaster.merge({'secondary' => true}),
      @ismaster.merge({'secondary' => true})
    )
    @client.local_manager.stubs(:refresh_required?).returns(true)
    @client.refresh

    assert_equal DEFAULT_MAX_BSON_SIZE, @client.max_bson_size
    assert_equal DEFAULT_MAX_BSON_SIZE * MESSAGE_SIZE_FACTOR, @client.max_message_size
    assert_equal 0, @client.max_wire_version
    assert_equal 0, @client.min_wire_version
  end

  def test_only_bson_size_in_config
    @db.stubs(:command).returns(
      @ismaster.merge({'ismaster' => true}),
      @ismaster.merge({'secondary' => true}),
      @ismaster.merge({'secondary' => true, 'maxBsonObjectSize' => 1024})
    )
    @client.local_manager.stubs(:refresh_required?).returns(true)
    @client.refresh

    assert_equal 1024, @client.max_bson_size
    assert_equal 1024 * MESSAGE_SIZE_FACTOR, @client.max_message_size
    assert_equal 0, @client.max_wire_version
    assert_equal 0, @client.min_wire_version
  end

  def test_values_in_config
    #ismaster is called three times on the first node
    @db.stubs(:command).returns(
      @ismaster.merge({'ismaster' => true, 'maxMessageSizeBytes' => 1024 * 2 * MESSAGE_SIZE_FACTOR,
                       'maxBsonObjectSize' => 1024, 'maxWireVersion' => 2, 'minWireVersion' => 1}),
      @ismaster.merge({'ismaster' => true, 'maxMessageSizeBytes' => 1024 * 2 * MESSAGE_SIZE_FACTOR,
                       'maxBsonObjectSize' => 1024, 'maxWireVersion' => 2, 'minWireVersion' => 1}),
      @ismaster.merge({'ismaster' => true, 'maxMessageSizeBytes' => 1024 * 2 * MESSAGE_SIZE_FACTOR,
                       'maxBsonObjectSize' => 1024, 'maxWireVersion' => 2, 'minWireVersion' => 1}),
      @ismaster.merge({'secondary' => true, 'maxMessageSizeBytes' => 1024 * 2 * MESSAGE_SIZE_FACTOR,
                       'maxBsonObjectSize' => 1024, 'maxWireVersion' => 2, 'minWireVersion' => 0}),
      @ismaster.merge({'secondary' => true, 'maxMessageSizeBytes' => 1024 * 2 * MESSAGE_SIZE_FACTOR,
                       'maxBsonObjectSize' => 1024, 'maxWireVersion' => 1, 'minWireVersion' => 0})
    )
    @client.local_manager.stubs(:refresh_required?).returns(true)
    @client.refresh

    assert_equal 1024, @client.max_bson_size
    assert_equal 1024 * 2 * MESSAGE_SIZE_FACTOR, @client.max_message_size
    assert_equal 1, @client.max_wire_version # minimum of all max_wire_version
    assert_equal 1, @client.min_wire_version # maximum of all min_wire_version
  end

  def test_wire_version_not_in_range
    min_wire_version, max_wire_version = [Mongo::MongoClient::MIN_WIRE_VERSION-1, Mongo::MongoClient::MIN_WIRE_VERSION-1]
    #ismaster is called three times on the first node
    @db.stubs(:command).returns(
      @ismaster.merge({'ismaster' => true, 'maxWireVersion' => max_wire_version, 'minWireVersion' => min_wire_version}),
      @ismaster.merge({'ismaster' => true, 'maxWireVersion' => max_wire_version, 'minWireVersion' => min_wire_version}),
      @ismaster.merge({'ismaster' => true, 'maxWireVersion' => max_wire_version, 'minWireVersion' => min_wire_version}),
      @ismaster.merge({'secondary' => true, 'maxWireVersion' => max_wire_version, 'minWireVersion' => min_wire_version}),
      @ismaster.merge({'secondary' => true, 'maxWireVersion' => max_wire_version, 'minWireVersion' => min_wire_version})
    )
    @client.local_manager.stubs(:refresh_required?).returns(true)
    assert_raises Mongo::ConnectionFailure do
      @client.refresh
    end
  end

  def test_use_write_command
    with_write_commands(@client) do
      assert_true @client.use_write_command?({:w => 1})
      assert_false @client.use_write_command?({:w => 0})
    end
    with_write_operations(@client) do
      assert_false @client.use_write_command?({:w => 1})
      assert_false @client.use_write_command?({:w => 0})
    end
    @client.local_manager.primary_pool.node.expects(:wire_version_feature?).at_least_once.returns(true)
    assert_true @client.use_write_command?({:w => 1})
    assert_false @client.use_write_command?({:w => 0})
  end

  def test_max_write_batch_size
    assert_equal Mongo::MongoClient::DEFAULT_MAX_WRITE_BATCH_SIZE, @client.max_write_batch_size
    @client.local_manager.primary_pool.node.stubs(:max_write_batch_size).returns(999)
    assert_equal 999, @client.max_write_batch_size
  end

  def test_max_write_batch_size_no_manager
    # Simulate no local manager being set yet - RUBY-759
    @client.stubs(:local_manager).returns(nil)
    assert_equal Mongo::MongoClient::DEFAULT_MAX_WRITE_BATCH_SIZE, @client.max_write_batch_size
  end
end

