require 'test_helper'

# mock test case to shutdown cluster for rake test:replica_set, must be last by file name sort order
class ZClusterShutdownTest < Test::Unit::TestCase
  def setup
    ensure_cluster(:rs)
  end

  def test_cluster_shutdown
    @@force_shutdown = true
  end
end

