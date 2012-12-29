require 'test_helper'
require 'mongo'

class PoolReuseTest < Test::Unit::TestCase
  include Mongo
  def setup
    ensure_cluster(:rs)
    @conn = MongoReplicaSetClient.new(["%s:%s" % [@rs.primary.host, @rs.primary.port]])
  end

  def count_open_file_handles
    @conn["admin"].command(:serverStatus => 1)["connections"]["current"]
  end

  def test_pool_resources_are_reused
    handles_before_refresh = count_open_file_handles
    10.times do
      @conn.hard_refresh!
    end
    assert_equal handles_before_refresh, count_open_file_handles
  end
end