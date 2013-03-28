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
        m.stubs(:config).returns({'maxMessageSizeBytes' => 1024 * 2.5, 'maxBsonObjectSize' => 1024})
        m.set_config
      end
    end
    # check that max sizes match what was changed
    assert_equal 1024 * 2.5, @client.max_message_size
    assert_equal 1024, @client.max_bson_size
  end

end