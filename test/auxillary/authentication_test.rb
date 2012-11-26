require 'test_helper'
require 'mongo'

# NOTE: This test requires bouncing the server.
# It also requires that a user exists on the admin database.
class AuthenticationTest < Test::Unit::TestCase
  include Mongo

  def setup
    @client = MongoClient.new
    @db1 = @client.db('mongo-ruby-test-auth1')
    @db2 = @client.db('mongo-ruby-test-auth2')
    @admin = @client.db('admin')
  end

  def teardown
    @db1.authenticate('user1', 'secret')
    @db2.authenticate('user2', 'secret')
    @client.drop_database('mongo-ruby-test-auth1')
    @client.drop_database('mongo-ruby-test-auth2')
  end

  def test_authenticate
    @admin.authenticate('bob', 'secret')
    @db1.add_user('user1', 'secret')
    @db2.add_user('user2', 'secret')
    @db2.add_user('userRO', 'secret', true) # read-only
    @admin.logout

    assert_raise Mongo::OperationFailure do
      @db1['stuff'].insert({:a => 2})
    end

    assert_raise Mongo::OperationFailure do
      @db2['stuff'].insert({:a => 2})
    end

    @db1.authenticate('user1', 'secret')
    @db2.authenticate('user2', 'secret')

    assert @db1['stuff'].insert({:a => 2})
    assert @db2['stuff'].insert({:a => 2})

    puts "Please bounce the server."
    gets

    # Here we reconnect.
    begin
      @db1['stuff'].find.to_a
      rescue Mongo::ConnectionFailure
    end

    assert @db1['stuff'].insert({:a => 2})
    assert @db2['stuff'].insert({:a => 2})
    assert @db2['stuff'].find({})

    @db1.logout
    assert_raise Mongo::OperationFailure do
      @db1['stuff'].insert({:a => 2})
    end

    @db2.logout
    assert_raise Mongo::OperationFailure do
      assert @db2['stuff'].insert({:a => 2})
    end

    @db2.authenticate('userRO', 'secret')
    assert @db2['stuff'].find({})
    assert_raise Mongo::OperationFailure do
      assert @db2['stuff'].insert({:a => 2})
    end
  end

end
