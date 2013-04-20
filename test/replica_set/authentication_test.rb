require 'test_helper'
require 'shared/authentication'

class ReplicaSetAuthenticationTest < Test::Unit::TestCase
  include Mongo
  include AuthenticationTests

  def setup
    ensure_cluster(:rs)
    @client = MongoReplicaSetClient.new(@rs.repl_set_seeds, :name => @rs.repl_set_name, :connect_timeout => 60)
    @db = @client[MONGO_TEST_DB]
    init_auth
  end

  def test_authenticate_with_connection_uri
    @db.add_user('eunice', 'uritest')
    assert MongoReplicaSetClient.from_uri(
      "mongodb://eunice:uritest@#{@rs.repl_set_seeds.join(',')}/#{@db.name}?replicaSet=#{@rs.repl_set_name}")
  end
end
