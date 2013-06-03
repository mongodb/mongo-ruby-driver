# Copyright (C) 2013 10gen Inc.
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
