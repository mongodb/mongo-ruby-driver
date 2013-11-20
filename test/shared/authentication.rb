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

module AuthenticationTests

  def init_auth
    silently do
      roles = { :roles => ['readAnyDatabase',
                           'readWriteAnyDatabase',
                           'userAdminAnyDatabase',
                           'dbAdminAnyDatabase',
                           'clusterAdmin'] }

      # enable authentication by creating and logging in as admin user
      @admin = @client['admin']
      @admin.add_user('admin', 'password', false, roles)
      @admin.authenticate('admin', 'password')

      # create a user who can remove all others in teardown
      @db.add_user('admin', 'password')
    end
  end

  def teardown
    remove_all_users(@admin, 'admin', 'password') if has_auth?(@admin.name)
    remove_all_users(@db, 'admin', 'password') if has_auth?(@db.name)
  end

  def remove_all_users(database, username, password)
    database.authenticate(username, password, false)
    if @client.server_version < '2.5'
      database['system.users'].remove
    else
      database.command(:dropAllUsersFromDatabase => 1)
    end
    database.logout
  end

  def has_auth?(db_name)
    @client.auths.any? { |a| a[:source] == db_name }
  end

  def test_add_remove_user
    silently do
      # add user
      @db.add_user('bob','user')
      assert @db.authenticate('bob', 'user')

      # remove user
      assert @db.remove_user('bob')
      @db.logout
    end
  end

  def test_update_user
    silently do
      # add user
      @db.add_user('bob', 'user')
      assert @db.authenticate('bob', 'user')
      @db.logout

      # update user
      @db.add_user('bob', 'updated')
      assert_raise Mongo::AuthenticationError do
        @db.authenticate('bob', 'user')
      end
      assert @db.authenticate('bob', 'updated')
      @db.logout
    end
  end

  def test_remove_non_existent_user
    if @client.server_version < '2.5'
      assert_equal false, @db.remove_user('joe')
    else
      assert_raise Mongo::OperationFailure do
        assert @db.remove_user('joe')
      end
    end
  end

  def test_authenticate
    silently do
      @db.add_user('peggy', 'user')
      assert @db.authenticate('peggy', 'user')
      @db.remove_user('peggy')
      @db.logout
    end
  end

  def test_authenticate_non_existent_user
    assert_raise Mongo::AuthenticationError do
      @db.authenticate('frank', 'thetank')
    end
  end

  def test_logout
    silently do
      @db.add_user('peggy', 'user')
      assert @db.authenticate('peggy', 'user')
      assert @db.logout
    end
  end

  def test_authenticate_with_special_characters
    silently do
      assert @db.add_user('foo:bar','@foo')
      assert @db.authenticate('foo:bar','@foo')
      @db.logout
    end
  end

  def test_authenticate_read_only
    silently do
      @db.add_user('randy', 'readonly', true)
      assert @db.authenticate('randy', 'readonly')
      @db.logout
    end
  end

  def test_non_admin_default_roles
    return if @client.server_version < '2.5'

    silently do
      # add read-only user and verify that role is 'read'
      @db.add_user('randy', 'password', nil, :roles => ['read'])
      @db.authenticate('randy', 'password')
      users = @db.command(:usersInfo => 'randy')['users']
      assert_equal 'read', users.first['roles'].first['role']
      @db.logout

      # add dbOwner (default) user and verify role
      @db.add_user('emily', 'password')
      @db.authenticate('emily', 'password')
      users = @db.command(:usersInfo => 'emily')['users']
      assert_equal 'dbOwner', users.first['roles'].first['role']
      @db.logout
    end
  end

  def test_socket_auths
    silently do
      # setup
      db_a = @client['test_a']
      db_a.add_user('user_a', 'password')
      assert db_a.authenticate('user_a', 'password')

      db_b = @client['test_b']
      db_b.add_user('user_b', 'password')
      assert db_b.authenticate('user_b', 'password')

      db_c = @client['test_c']
      db_c.add_user('user_c', 'password')
      assert db_c.authenticate('user_c', 'password')

      # client auths should be applied to socket on checkout
      socket = @client.checkout_reader(:mode => :primary)
      assert_equal 4, socket.auths.size
      assert_equal @client.auths, socket.auths
      @client.checkin(socket)

      # logout should remove saved auth on socket and client
      assert db_b.logout
      socket = @client.checkout_reader(:mode => :primary)
      assert_equal 3, socket.auths.size
      assert_equal @client.auths, socket.auths
      @client.checkin(socket)

      # clean-up
      db_b.authenticate('user_b', 'password')
      remove_all_users(db_a, 'user_a', 'password')
      remove_all_users(db_b, 'user_b', 'password')
      remove_all_users(db_c, 'user_c', 'password')
    end
  end

  def test_delegated_authentication
    return unless @client.server_version >= '2.4' && @client.server_version < '2.5'

    silently do
      # create user in source database
      accounts = @client['accounts']
      accounts.add_user('debbie', 'delegate')

      # add user to test database
      @db.add_user('debbie', nil, nil, :roles => ['read'], :userSource => 'accounts')
      @admin.logout

      # auth must occur on the source database
      assert_raise Mongo::AuthenticationError do
        @db.authenticate('debbie', 'delegate')
      end

      # validate direct auth
      assert accounts.authenticate('debbie', 'delegate')
      assert @db.collection_names
      accounts.logout
      assert_raise Mongo::OperationFailure do
        @db.collection_names
      end

      # validate auth using source database
      @db.authenticate('debbie', 'delegate', true, 'accounts')
      assert @db.collection_names
      accounts.logout
      assert_raise Mongo::OperationFailure do
        @db.collection_names
      end

      # clean-up
      @admin.authenticate('admin', 'password')
      remove_all_users(accounts, 'debbie', 'delegate')
    end
  end

  # Tests for the PLAIN (LDAP) Authentication Mechanism.
  #
  # Note: These tests will be skipped automatically unless the test environment
  # has been configured.
  #
  # In order to run these tests, set the following environment variables:
  #
  #   export MONGODB_SASL_HOST='server.domain.com'
  #   export MONGODB_SASL_USER='application%2Fuser%40example.com'
  #   export MONGODB_SASL_PASS='password'
  #
  #   # optional (defaults to '$external')
  #   export MONGODB_SASL_SOURCE='source_database'
  #
  if ENV.key?('MONGODB_SASL_HOST') && ENV.key?('MONGODB_SASL_USER') && ENV.key?('MONGODB_SASL_PASS')

    def test_plain_authenticate
      replica_set = @client.class.name == 'Mongo::MongoReplicaSetClient'

      # TODO: Remove this once we have a replica set configured for SASL in CI
      return if ENV.key?('CI') && replica_set

      host   = replica_set ? [ENV['MONGODB_SASL_HOST']] : ENV['MONGODB_SASL_HOST']
      client = @client.class.new(host)
      source = ENV['MONGODB_SASL_SOURCE'] || '$external'
      db     = client['test']

      # should successfully authenticate
      assert db.authenticate(ENV['MONGODB_SASL_USER'], ENV['MONGODB_SASL_PASS'], true, source, 'PLAIN')
      assert client[source].logout

      # should raise on missing password
      ex = assert_raise Mongo::MongoArgumentError do
        db.authenticate(ENV['MONGODB_SASL_USER'], nil, true, source, 'PLAIN')
      end
      assert_match /username and password are required/, ex.message

      # should raise on invalid password
      assert_raise Mongo::AuthenticationError do
        db.authenticate(ENV['MONGODB_SASL_USER'], 'foo', true, source, 'PLAIN')
      end
    end

    def test_plain_authenticate_from_uri
      source = ENV['MONGODB_SASL_SOURCE'] || '$external'

      uri    = "mongodb://#{ENV['MONGODB_SASL_USER']}:#{ENV['MONGODB_SASL_PASS']}@" +
               "#{ENV['MONGODB_SASL_HOST']}/some_db?authSource=#{source}" +
               "&authMechanism=PLAIN"

      client = @client.class.from_uri(uri)
      db     = client['test']

      # should be able to checkout a socket (authentication gets applied)
      assert socket = client.checkout_reader(:mode => :primary)
      client[source].logout(:socket => socket)
      client.checkin(socket)

      uri = "mongodb://#{ENV['MONGODB_SASL_USER']}@#{ENV['MONGODB_SASL_HOST']}/" +
            "some_db?authSource=#{source}&authMechanism=PLAIN"

      # should raise for missing password
      ex = assert_raise Mongo::MongoArgumentError do
        client = @client.class.from_uri(uri)
      end
      assert_match /username and password are required/, ex.message

      uri = "mongodb://#{ENV['MONGODB_SASL_USER']}:foo@#{ENV['MONGODB_SASL_HOST']}/" +
            "some_db?authSource=#{source}&authMechanism=PLAIN"

      # should raise for invalid password
      client = @client.class.from_uri(uri)
      assert_raise Mongo::AuthenticationError do
        client.checkout_reader(:mode => :primary)
      end
    end

  end
end
