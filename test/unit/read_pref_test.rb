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

  def test_sok_text_returns_true
    command = BSON::OrderedHash['text', BSON::OrderedHash['search', 'coffee']]
    assert_equal true, ReadPreference::secondary_ok?(command)
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

  def test_primary_with_tags_raises_error
    # Confirm that an error is raised if you provide tags and read pref is primary
    client = MongoReplicaSetClient.new(["#{TEST_HOST}:#{TEST_PORT}"], :connect => false)
    client.stubs(:primary_pool).returns(mock_pool)
    read_pref_tags = {'dc' => 'nyc'}
    read_pref = client.read_preference.merge(:mode    => :primary,
                                             :tags    => [read_pref_tags],
                                             :latency => 6)
    assert_raise Mongo::MongoArgumentError do
      client.select_pool(read_pref)
    end
  end

  def test_secondary_pref
    # Confirm that a primary is not selected
    primary_pool = mock('pool')

    secondary_nyc = mock_pool({'dc' => 'nyc'}, 5)
    secondary_chi = mock_pool({'dc' => 'chicago'}, 5)
    secondary_pools = [secondary_nyc, secondary_chi]

    client = MongoReplicaSetClient.new(["#{TEST_HOST}:#{TEST_PORT}"], :connect => false)
    client.stubs(:primary_pool).returns(primary_pool)
    client.stubs(:secondary_pools).returns(secondary_pools)

    read_pref = client.read_preference.merge(:mode => :secondary)
    assert_not_equal Hash.new, client.select_pool(read_pref).tags
  end

  def test_secondary_tags_pref
    # Confirm that a secondary with matching tags is selected
    secondary_nyc = mock_pool({'dc' => 'nyc'}, 5)
    secondary_chi = mock_pool({'dc' => 'chicago'}, 5)
    secondary_pools = [secondary_nyc, secondary_chi]

    client = MongoReplicaSetClient.new(["#{TEST_HOST}:#{TEST_PORT}"], :connect => false)
    client.stubs(:primary_pool).returns(mock_pool)
    client.stubs(:secondary_pools).returns(secondary_pools)

    read_pref_tags = {'dc' => 'nyc'}
    read_pref = client.read_preference.merge(:mode => :secondary,
                                             :tags => [read_pref_tags])
    assert_equal read_pref_tags, client.select_pool(read_pref).tags
  end

  def test_secondary_tags_with_latency
    # Confirm that between more than 1 secondary matching tags, only the one within
    # max acceptable latency is selected
    secondary_nyc = mock_pool({'dc' => 'nyc'}, 5)
    secondary_nyc2 = mock_pool({'dc' => 'nyc'}, 25)
    secondary_pools = [secondary_nyc, secondary_nyc2]

    client = MongoReplicaSetClient.new(["#{TEST_HOST}:#{TEST_PORT}"], :connect => false)
    client.stubs(:secondary_pools).returns(secondary_pools)

    read_pref_tags = {'dc' => 'nyc'}
    read_pref = client.read_preference.merge(:mode => :secondary,
                                             :tags => [read_pref_tags])
    assert_equal 5, client.select_pool(read_pref).ping_time
  end

  def test_secondary_latency_pref
    # Confirm that only the latency of pools matching tags is considered
    secondary_nyc = mock_pool({'dc' => 'nyc'}, 10)
    secondary_chi = mock_pool({'dc' => 'chicago'}, 5)
    secondary_pools = [secondary_nyc, secondary_chi]

    client = MongoReplicaSetClient.new(["#{TEST_HOST}:#{TEST_PORT}"], :connect => false)
    client.stubs(:primary_pool).returns(mock_pool)
    client.stubs(:secondary_pools).returns(secondary_pools)

    read_pref_tags = {'dc' => 'nyc'}
    read_pref = client.read_preference.merge(:mode    => :secondary,
                                             :tags    => [read_pref_tags],
                                             :latency => 3)
    assert_equal read_pref_tags, client.select_pool(read_pref).tags
  end

  def test_primary_preferred_primary_available
    # Confirm that the primary is always selected if it's available
    secondary_nyc = mock_pool({'dc' => 'nyc'}, 5)
    secondary_chi = mock_pool({'dc' => 'chicago'}, 10)
    secondary_pools = [secondary_nyc, secondary_chi]
    primary_pool = mock_pool

    client = MongoReplicaSetClient.new(["#{TEST_HOST}:#{TEST_PORT}"], :connect => false)
    client.stubs(:secondary_pools).returns(secondary_pools)
    client.stubs(:primary_pool).returns(primary_pool)

    read_pref_tags = {'dc' => 'chicago'}
    read_pref = client.read_preference.merge(:mode    => :primary_preferred,
                                             :tags    => [read_pref_tags],
                                             :latency => 6)
    assert_equal primary_pool, client.select_pool(read_pref)
  end

  def test_primary_preferred_primary_not_available
    # Confirm that a secondary with matching tags is selected if primary is
    # not available
    secondary_nyc = mock_pool({'dc' => 'nyc'}, 5)
    secondary_chi = mock_pool({'dc' => 'chicago'}, 10)
    secondary_pools = [secondary_nyc, secondary_chi]

    client = MongoReplicaSetClient.new(["#{TEST_HOST}:#{TEST_PORT}"], :connect => false)
    client.stubs(:secondary_pools).returns(secondary_pools)

    read_pref_tags = {'dc' => 'chicago'}
    read_pref = client.read_preference.merge(:mode    => :primary_preferred,
                                             :tags    => [read_pref_tags],
                                             :latency => 6)
    assert_equal read_pref_tags, client.select_pool(read_pref).tags
  end

  def test_primary_preferred_primary_not_available_and_no_matching_tags
    # Confirm that tags are taken into account if primary is not available and
    # secondaries are considered for selection.
    secondary_nyc = mock_pool({'dc' => 'nyc'}, 5)
    secondary_chi = mock_pool({'dc' => 'chicago'}, 10)
    secondary_pools = [secondary_nyc, secondary_chi]

    client = MongoReplicaSetClient.new(["#{TEST_HOST}:#{TEST_PORT}"], :connect => false)
    client.stubs(:secondary_pools).returns(secondary_pools)

    read_pref_tags = {'dc' => 'other_city'}
    read_pref = client.read_preference.merge(:mode    => :primary_preferred,
                                             :tags    => [read_pref_tags],
                                             :latency => 6)
    assert_equal nil, client.select_pool(read_pref)
  end

  def test_secondary_preferred_with_tags
    # Confirm that tags are taken into account
    secondary_nyc = mock_pool({'dc' => 'nyc'}, 5)
    secondary_chi = mock_pool({'dc' => 'chicago'}, 10)
    secondary_pools = [secondary_nyc, secondary_chi]

    client = MongoReplicaSetClient.new(["#{TEST_HOST}:#{TEST_PORT}"], :connect => false)
    client.stubs(:secondary_pools).returns(secondary_pools)

    read_pref_tags = {'dc' => 'chicago'}
    read_pref = client.read_preference.merge(:mode    => :secondary_preferred,
                                             :tags    => [read_pref_tags],
                                             :latency => 6)
    assert_equal read_pref_tags, client.select_pool(read_pref).tags
  end

  def test_secondary_preferred_with_no_matching_tags
    # Confirm that the primary is selected if no secondaries with matching tags
    # are found
    secondary_nyc = mock_pool({'dc' => 'nyc'}, 5)
    secondary_chi = mock_pool({'dc' => 'chicago'}, 10)
    secondary_pools = [secondary_nyc, secondary_chi]

    client = MongoReplicaSetClient.new(["#{TEST_HOST}:#{TEST_PORT}"], :connect => false)
    client.stubs(:secondary_pools).returns(secondary_pools)
    client.stubs(:primary_pool).returns(mock_pool)

    read_pref_tags = {'dc' => 'other_city'}
    read_pref = client.read_preference.merge(:mode    => :secondary_preferred,
                                             :tags    => [read_pref_tags],
                                             :latency => 6)
    assert_equal Hash.new, client.select_pool(read_pref).tags
  end

  def test_nearest_with_tags
    # Confirm that tags are taken into account when selecting nearest
    secondary_nyc = mock_pool({'dc' => 'nyc'}, 5)
    secondary_chi = mock_pool({'dc' => 'chicago'}, 10)
    primary_pool = mock_pool
    pools = [secondary_nyc, secondary_chi, primary_pool]

    client = MongoReplicaSetClient.new(["#{TEST_HOST}:#{TEST_PORT}"], :connect => false)
    client.stubs(:pools).returns(pools)

    read_pref_tags = {'dc' => 'nyc'}
    read_pref = client.read_preference.merge(:mode    => :nearest,
                                             :tags    => [read_pref_tags],
                                             :latency => 3)
    assert_equal read_pref_tags, client.select_pool(read_pref).tags
  end

  def test_nearest
    # Confirm that the nearest is selected when tags aren't specified
    secondary_nyc = mock_pool({'dc' => 'nyc'}, 5)
    secondary_chi = mock_pool({'dc' => 'chicago'}, 10)
    primary_pool = mock_pool({}, 1)
    pools = [secondary_nyc, secondary_chi, primary_pool]

    client = MongoReplicaSetClient.new(["#{TEST_HOST}:#{TEST_PORT}"], :connect => false)
    client.stubs(:pools).returns(pools)

    read_pref = client.read_preference.merge(:mode    => :nearest,
                                             :latency => 3)
    assert_equal Hash.new, client.select_pool(read_pref).tags
  end

  def test_nearest_primary_matching
    # Confirm that a primary matching tags is included in nearest candidates
    secondary_nyc = mock_pool({'dc' => 'nyc'}, 5)
    secondary_chi = mock_pool({'dc' => 'chicago'}, 10)
    primary_pool = mock_pool({'dc' => 'boston'}, 1)
    secondary_pools = [secondary_nyc, secondary_chi]

    client = MongoReplicaSetClient.new(["#{TEST_HOST}:#{TEST_PORT}"], :connect => false)
    client.stubs(:secondary_pools).returns(secondary_pools)
    client.stubs(:primary_pool).returns(primary_pool)
    client.stubs(:pools).returns(secondary_pools << primary_pool)

    read_pref_tags = {'dc' => 'boston'}
    read_pref = client.read_preference.merge(:mode    => :nearest,
                                             :tags    => [read_pref_tags])
    assert_equal primary_pool, client.select_pool(read_pref)
  end

  def test_nearest_primary_not_matching
    # Confirm that a primary not matching tags is not included in nearest candidates
    secondary_nyc = mock_pool({'dc' => 'nyc'}, 25)
    secondary_chi = mock_pool({'dc' => 'chicago'}, 25)
    primary_pool = mock_pool({'dc' => 'boston'}, 1)
    secondary_pools = [secondary_nyc, secondary_chi]

    client = MongoReplicaSetClient.new(["#{TEST_HOST}:#{TEST_PORT}"], :connect => false)
    client.stubs(:secondary_pools).returns(secondary_pools)
    client.stubs(:primary_pool).returns(mock_pool)
    client.stubs(:pools).returns(secondary_pools << primary_pool)

    read_pref_tags = {'dc' => 'SF'}
    read_pref = client.read_preference.merge(:mode    => :nearest,
                                             :tags    => [read_pref_tags])
    assert_equal nil, client.select_pool(read_pref)
  end

  def test_nearest_primary_not_matching_excluded_from_latency_calculations
    # Confirm that a primary not matching tags is not included in the latency calculations
    secondary1 = mock_pool({'dc' => 'nyc'}, 10)
    secondary2 = mock_pool({'dc' => 'nyc'}, 10)
    primary_pool = mock_pool({'dc' => 'boston'}, 1)
    secondary_pools = [secondary1, secondary2]

    client = MongoReplicaSetClient.new(["#{TEST_HOST}:#{TEST_PORT}"], :connect => false)
    client.stubs(:secondary_pools).returns(secondary_pools)
    client.stubs(:primary_pool).returns(mock_pool)
    client.stubs(:pools).returns(secondary_pools << primary_pool)

    read_pref_tags = {'dc' => 'nyc'}
    read_pref = client.read_preference.merge(:mode    => :nearest,
                                             :tags    => [read_pref_tags],
                                             :latency => 5)
    assert_equal 'nyc', client.select_pool(read_pref).tags['dc']
  end

  def test_nearest_matching_tags_but_not_available
    # Confirm that even if a server matches a tag, it's not selected if it's down
    secondary_nyc = mock_pool({'dc' => 'nyc'}, 5)
    secondary_chi = mock_pool({'dc' => 'chicago'}, 10)
    primary_pool = mock_pool({'dc' => 'chicago'}, nil)
    secondary_pools = [secondary_nyc, secondary_chi]

    client = MongoReplicaSetClient.new(["#{TEST_HOST}:#{TEST_PORT}"], :connect => false)
    client.stubs(:secondary_pools).returns(secondary_pools)
    client.stubs(:primary_pool).returns(primary_pool)
    client.stubs(:pools).returns(secondary_pools << primary_pool)

    tags = [{'dc' => 'nyc'}, {'dc' => 'chicago'}, {}]
    read_pref = client.read_preference.merge(:mode    => :nearest,
                                             :tags    => tags)
    assert_equal secondary_nyc, client.select_pool(read_pref)
  end

  def test_nearest_multiple_tags
    # Confirm that with multiple tags in the read preference, servers are still selected
    secondary_nyc = mock_pool({}, 5)
    secondary_chi = mock_pool({'dc' => 'chicago'}, 10)
    primary_pool = mock_pool({}, 1)
    secondary_pools = [secondary_nyc, secondary_chi]

    client = MongoReplicaSetClient.new(["#{TEST_HOST}:#{TEST_PORT}"], :connect => false)
    client.stubs(:secondary_pools).returns(secondary_pools)
    client.stubs(:primary_pool).returns(mock_pool)
    client.stubs(:pools).returns(secondary_pools << primary_pool)

    tags = [{'dc' => 'nyc'}, {'dc' => 'chicago'}, {}]
    read_pref = client.read_preference.merge(:mode    => :nearest,
                                             :tags    => tags)
    assert_equal secondary_chi, client.select_pool(read_pref)
  end
end
