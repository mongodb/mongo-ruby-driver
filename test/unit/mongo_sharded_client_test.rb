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

require "test_helper"

class MongoShardedClientUnitTest < Test::Unit::TestCase
  include Mongo

  def test_initialize_with_single_mongos_uri
    uri = "mongodb://localhost:27017"
    with_preserved_env_uri(uri) do
      client = MongoShardedClient.new(:connect => false)
      assert_equal [[ "localhost", 27017 ]], client.seeds
    end
  end

  def test_initialize_with_multiple_mongos_uris
    uri = "mongodb://localhost:27017,localhost:27018"
    with_preserved_env_uri(uri) do
      client = MongoShardedClient.new(:connect => false)
      assert_equal [[ "localhost", 27017 ], [ "localhost", 27018 ]], client.seeds
    end
  end

  def test_from_uri_with_string
    client = MongoShardedClient.from_uri("mongodb://localhost:27017,localhost:27018", :connect => false)
    assert_equal [[ "localhost", 27017 ], [ "localhost", 27018 ]], client.seeds
  end

  def test_from_uri_with_env_variable
    uri = "mongodb://localhost:27017,localhost:27018"
    with_preserved_env_uri(uri) do
      client = MongoShardedClient.from_uri(nil, :connect => false)
      assert_equal [[ "localhost", 27017 ], [ "localhost", 27018 ]], client.seeds
    end
  end
end
