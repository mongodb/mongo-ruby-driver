require 'test_helper'
require 'mongo'

class AuthTest < Test::Unit::TestCase
  include Mongo

  def setup
    @rs = AuthReplSetManager.new(:start_port => 40000)
    @rs.start_set
  end

  def teardown
    #@rs.cleanup_set
  end

  def test_repl_set_auth
    @client = MongoReplicaSetClient.new(build_seeds(3), :name => @rs.name)

    # Add an admin user
    @client['admin'].add_user("me", "secret")

    # Ensure that insert fails
    assert_raise_error Mongo::OperationFailure, "unauthorized" do
      @client['foo']['stuff'].insert({:a => 2}, {:w => 2})
    end

    # Then authenticate
    assert @client['admin'].authenticate("me", "secret")

    # Insert should succeed now
    assert @client['foo']['stuff'].insert({:a => 2}, {:w => 2})

    # So should a query
    assert @client['foo']['stuff'].find_one

    # But not when we logout
    @client['admin'].logout

    assert_raise_error Mongo::OperationFailure, "unauthorized" do
      @client['foo']['stuff'].find_one
    end

    # Same should apply to a random secondary
    @slave1 = MongoClient.new(@client.secondary_pools[0].host,
      @client.secondary_pools[0].port, :slave_ok => true)

    # Find should fail
    assert_raise_error Mongo::OperationFailure, "unauthorized" do
      @slave1['foo']['stuff'].find_one
    end

    # But not when authenticated
    assert @slave1['admin'].authenticate("me", "secret")
    assert @slave1['foo']['stuff'].find_one

    # Same should apply when using :secondary_only
    @second_only = MongoReplicaSetClient.new(build_seeds(3), 
      :require_primary => false, :read => :secondary_only)

    # Find should fail
    assert_raise_error Mongo::OperationFailure, "unauthorized" do
      @second_only['foo']['stuff'].find_one
    end

    # But not when authenticated
    assert @second_only['admin'].authenticate("me", "secret")
    assert @second_only['foo']['stuff'].find_one
  end
end
