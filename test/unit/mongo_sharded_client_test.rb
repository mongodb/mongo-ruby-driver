require "test_helper"

class MongoShardedClientTest < Test::Unit::TestCase
  include Mongo

  def setup
    ENV["MONGODB_URI"] = nil
  end

  def test_initialize_with_single_mongos_uri
    ENV["MONGODB_URI"] = "mongodb://localhost:27017"
    client = MongoShardedClient.new(:connect => false)
    assert_equal [[ "localhost", 27017 ]], client.seeds
  end

  def test_initialize_with_multiple_mongos_uris
    ENV["MONGODB_URI"] = "mongodb://localhost:27017,localhost:27018"
    client = MongoShardedClient.new(:connect => false)
    assert_equal [[ "localhost", 27017 ], [ "localhost", 27018 ]], client.seeds
  end

  def test_from_uri_with_string
    client = MongoShardedClient.from_uri("mongodb://localhost:27017,localhost:27018", :connect => false)
    assert_equal [[ "localhost", 27017 ], [ "localhost", 27018 ]], client.seeds
  end

  def test_from_uri_with_env_variable
    ENV["MONGODB_URI"] = "mongodb://localhost:27017,localhost:27018"
    client = MongoShardedClient.from_uri(nil, :connect => false)
    assert_equal [[ "localhost", 27017 ], [ "localhost", 27018 ]], client.seeds
  end
end
