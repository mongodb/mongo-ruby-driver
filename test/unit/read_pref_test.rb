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

class ReadPreferenceUnitTest < Test::Unit::TestCase

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

  def test_sok_mapreduce_out_string_returns_false
    command = BSON::OrderedHash['mapreduce', 'test-collection',
                                'out', 'new-test-collection']
    assert_equal false, ReadPreference::secondary_ok?(command)
  end

  def test_sok_mapreduce_replace_collection_returns_false
    command = BSON::OrderedHash['mapreduce', 'test-collection',
                                'out', BSON::OrderedHash['replace', 'new-test-collection']]
    assert_equal false, ReadPreference::secondary_ok?(command)
  end

  def test_sok_mapreduce_inline_collection_returns_false
    command = BSON::OrderedHash['mapreduce', 'test-collection',
                                'out', 'inline']
    assert_equal false, ReadPreference::secondary_ok?(command)
  end

  def test_sok_inline_symbol_mapreduce_returns_true
    command = BSON::OrderedHash['mapreduce', 'test-collection',
                                'out', BSON::OrderedHash[:inline, 'true']]
    assert_equal true, ReadPreference::secondary_ok?(command)
  end

  def test_sok_inline_string_mapreduce_returns_true
    command = BSON::OrderedHash['mapreduce', 'test-collection',
                                'out', BSON::OrderedHash['inline', 'true']]
    assert_equal true, ReadPreference::secondary_ok?(command)
  end

  def test_sok_count_true
    command = BSON::OrderedHash['count', 'test-collection',
                                'query', BSON::OrderedHash['a', 'b']]
    assert_equal true, ReadPreference::secondary_ok?(command)
  end

  def test_sok_server_status_returns_false
    command = BSON::OrderedHash['serverStatus', 1]
    assert_equal false, ReadPreference::secondary_ok?(command)
  end

  def test_cmd_reroute_with_secondary
    ReadPreference::expects(:warn).with(regexp_matches(/rerouted to primary/))
    command = BSON::OrderedHash['mapreduce', 'test-collection',
                                'out', 'new-test-collection']
    assert_equal :primary, ReadPreference::cmd_read_pref(:secondary, command)
  end

  def test_find_and_modify_reroute_with_secondary
    ReadPreference::expects(:warn).with(regexp_matches(/rerouted to primary/))
    command = BSON::OrderedHash['findAndModify', 'test-collection',
                                'query', {}]
    assert_equal :primary, ReadPreference::cmd_read_pref(:secondary, command)
  end

  def test_cmd_no_reroute_with_secondary
    command = BSON::OrderedHash['mapreduce', 'test-collection',
                                'out', BSON::OrderedHash['inline', 'true']]
    assert_equal :secondary, ReadPreference::cmd_read_pref(:secondary, command)
  end

  def test_cmd_no_reroute_with_primary
    command = BSON::OrderedHash['mapreduce', 'test-collection',
                                'out', 'new-test-collection']
    assert_equal :primary, ReadPreference::cmd_read_pref(:primary, command)
  end

  def test_cmd_no_reroute_with_primary_secondary_ok
    command = BSON::OrderedHash['mapreduce', 'test-collection',
                                'out', BSON::OrderedHash['inline', 'true']]
    assert_equal :primary, ReadPreference::cmd_read_pref(:primary, command)
  end

  def test_parallel_scan_secondary_ok
    command = BSON::OrderedHash['parallelCollectionScan', 'test-collection',
                                'numCursors', 3]
    assert_equal true, ReadPreference::secondary_ok?(command)
  end

end
