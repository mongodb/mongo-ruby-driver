require 'test_helper'
require 'shared/authentication'

class AuthenticationTest < Test::Unit::TestCase
  include Mongo
  include AuthenticationTests

  def setup
    @client = MongoClient.new
    @db     = @client[MONGO_TEST_DB]
    init_auth
  end

  def test_authenticate_with_connection_uri
    @db.add_user('eunice', 'uritest')
    assert MongoClient.from_uri("mongodb://eunice:uritest@#{host_port}/#{@db.name}")
  end
end
