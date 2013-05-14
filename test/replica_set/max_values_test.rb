require 'test_helper'

class MaxValuesTest < Test::Unit::TestCase

  include Mongo

  def setup
    ensure_cluster(:rs)
    @client = MongoReplicaSetClient.new(@rs.repl_set_seeds, :name => @rs.repl_set_name)
    @db = new_mock_db
    @client.stubs(:[]).returns(@db)
    @ismaster = {
      'hosts' => @client.local_manager.hosts.to_a,
      'arbiters' => @client.local_manager.arbiters
    }
  end

  def test_initial_max_sizes
    assert @client.max_message_size
    assert @client.max_bson_size
  end

  def test_updated_max_sizes_after_node_config_change
    @db.stubs(:command).returns(
      @ismaster.merge({'ismaster' => true}),
      @ismaster.merge({'secondary' => true, 'maxMessageSizeBytes' => 1024 * MESSAGE_SIZE_FACTOR}),
      @ismaster.merge({'secondary' => true, 'maxBsonObjectSize' => 1024})
    )
    @client.local_manager.stubs(:refresh_required?).returns(true)
    @client.refresh

    assert_equal 1024, @client.max_bson_size
    assert_equal 1024 * MESSAGE_SIZE_FACTOR, @client.max_message_size
  end

  def test_neither_max_sizes_in_config
    @db.stubs(:command).returns(
      @ismaster.merge({'ismaster' => true}),
      @ismaster.merge({'secondary' => true}),
      @ismaster.merge({'secondary' => true})
    )
    @client.local_manager.stubs(:refresh_required?).returns(true)
    @client.refresh

    assert_equal DEFAULT_MAX_BSON_SIZE, @client.max_bson_size
    assert_equal DEFAULT_MAX_BSON_SIZE * MESSAGE_SIZE_FACTOR, @client.max_message_size
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
  end

  def test_both_sizes_in_config
    @db.stubs(:command).returns(
      @ismaster.merge({'ismaster' => true, 'maxMessageSizeBytes' => 1024 * 2 * MESSAGE_SIZE_FACTOR, 'maxBsonObjectSize' => 1024}),
      @ismaster.merge({'secondary' => true, 'maxMessageSizeBytes' => 1024 * 2 * MESSAGE_SIZE_FACTOR, 'maxBsonObjectSize' => 1024}),
      @ismaster.merge({'secondary' => true, 'maxMessageSizeBytes' => 1024 * 2 * MESSAGE_SIZE_FACTOR, 'maxBsonObjectSize' => 1024})
    )
    @client.local_manager.stubs(:refresh_required?).returns(true)
    @client.refresh

    assert_equal 1024, @client.max_bson_size
    assert_equal 1024 * 2 * MESSAGE_SIZE_FACTOR, @client.max_message_size
  end

end

