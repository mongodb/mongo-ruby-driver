require 'test_helper'

class MaxValuesTest < Test::Unit::TestCase

  include Mongo

  def setup
    ensure_cluster(:rs)
    @client = MongoReplicaSetClient.new(@rs.repl_set_seeds, :name => @rs.repl_set_name)
  end

  def test_initial_max_sizes
    assert @client.max_message_size
    assert @client.max_bson_size
  end

  def test_updated_max_sizes_after_node_config_change
    # stub max sizes for one member
    @client.local_manager.members.each_with_index do |m, i|
      if i % 2 == 0
        m.stubs(:config).returns({'maxMessageSizeBytes' => 1024 * MESSAGE_SIZE_FACTOR, 'maxBsonObjectSize' => 1024})
        m.set_config
      end
    end

    assert_equal 1024, @client.max_bson_size
    assert_equal 1024 * MESSAGE_SIZE_FACTOR, @client.max_message_size
  end

  def test_neither_max_sizes_in_config
    @client.local_manager.members.each do |m|
      m.stubs(:config).returns({})
      m.set_config
    end

    assert_equal DEFAULT_MAX_BSON_SIZE, @client.max_bson_size
    assert_equal DEFAULT_MAX_BSON_SIZE * MESSAGE_SIZE_FACTOR, @client.max_message_size
  end

  def test_only_bson_size_in_config
    @client.local_manager.members.each do |m|
      m.stubs(:config).returns({'maxBsonObjectSize' => 1024})
      m.set_config
    end
    assert_equal 1024, @client.max_bson_size
    assert_equal 1024 * MESSAGE_SIZE_FACTOR, @client.max_message_size
  end

  def test_both_sizes_in_config
    @client.local_manager.members.each do |m|
      m.stubs(:config).returns({'maxMessageSizeBytes' => 1024 * 2 * MESSAGE_SIZE_FACTOR,
                                'maxBsonObjectSize' => 1024})
      m.set_config
    end

    assert_equal 1024, @client.max_bson_size
    assert_equal 1024 * 2 * MESSAGE_SIZE_FACTOR, @client.max_message_size
  end

end

