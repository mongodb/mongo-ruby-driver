require 'test_helper'

class ReadPrefTest < Test::Unit::TestCase
  include ReadPreference

  def setup
    mock_pool = mock()
    mock_pool.stubs(:ping_time).returns(Pool::MAX_PING_TIME)

    stubs(:primary_pool).returns(mock_pool)
    stubs(:secondary_pools).returns([mock_pool])
    stubs(:pools).returns([mock_pool])
  end

  def test_select_pool
    ReadPreference::READ_PREFERENCES.map do |rp|
      assert select_pool({:mode => rp, :tags => [], :latency => 15})
    end
  end

end
