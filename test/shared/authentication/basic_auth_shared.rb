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

module BasicAuthTests

  def init_auth_basic
    # enable authentication by creating and logging in as admin user
    @admin = @client['admin']
    @admin.add_user('admin', 'password', nil, :roles => ['readAnyDatabase',
                                                         'readWriteAnyDatabase',
                                                         'userAdminAnyDatabase',
                                                         'dbAdminAnyDatabase',
                                                         'clusterAdmin'])
    @admin.authenticate('admin', 'password')

    # db user for cleanup (for pre-2.4)
    @db.add_user('admin', 'cleanup', nil, :roles => [])
  end

  def teardown_basic
    remove_all_users(@db, 'admin', 'cleanup')
    remove_all_users(@admin, 'admin', 'password') if has_auth?(@admin.name)
  end

  def remove_all_users(database, username, password)
    database.authenticate(username, password) unless has_auth?(database.name)
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
    init_auth_basic

    # add user
    silently { @db.add_user('bob','user') }
    assert @db.authenticate('bob', 'user')

    # remove user
    assert @db.remove_user('bob')

    teardown_basic
  end

  def test_update_user
    init_auth_basic

    # add user
    silently { @db.add_user('bob', 'user') }
    assert @db.authenticate('bob', 'user')
    @db.logout

    # update user
    silently { @db.add_user('bob', 'updated') }
    assert_raise Mongo::AuthenticationError do
      @db.authenticate('bob', 'user')
    end
    assert @db.authenticate('bob', 'updated')

    teardown_basic
  end

  def test_remove_non_existent_user
    init_auth_basic

    if @client.server_version < '2.5'
      assert_equal false, @db.remove_user('joe')
    else
      assert_raise Mongo::OperationFailure do
        assert @db.remove_user('joe')
      end
    end
    teardown_basic
  end

  def test_authenticate
    init_auth_basic
    silently { @db.add_user('peggy', 'user') }
    assert @db.authenticate('peggy', 'user')
    @db.remove_user('peggy')
    teardown_basic
  end

  def test_authenticate_non_existent_user
    init_auth_basic
    assert_raise Mongo::AuthenticationError do
      @db.authenticate('frank', 'thetank')
    end
    teardown_basic
  end

  def test_logout
    init_auth_basic
    silently { @db.add_user('peggy', 'user') }
    assert @db.authenticate('peggy', 'user')
    assert @db.logout
    teardown_basic
  end

  def test_authenticate_with_special_characters
    init_auth_basic
    silently { assert @db.add_user('foo:bar','@foo') }
    assert @db.authenticate('foo:bar','@foo')
    teardown_basic
  end

  def test_authenticate_read_only
    init_auth_basic
    silently { @db.add_user('randy', 'readonly', true) }
    assert @db.authenticate('randy', 'readonly')
    teardown_basic
  end

  def test_authenticate_with_connection_uri
    init_auth_basic
    silently { @db.add_user('eunice', 'uritest') }

    uri    = "mongodb://eunice:uritest@#{@host_info}/#{@db.name}"
    client = Mongo::URIParser.new(uri).connection

    assert client
    assert_equal client.auths.size, 1
    assert client[TEST_DB]['auth_test'].count

    auth = client.auths.first
    assert_equal @db.name, auth[:db_name]
    assert_equal 'eunice', auth[:username]
    assert_equal 'uritest', auth[:password]
    teardown_basic
  end

  def test_socket_auths
    init_auth_basic
    # setup
    db_a = @client[TEST_DB + '_a']
    silently { db_a.add_user('user_a', 'password') }
    assert db_a.authenticate('user_a', 'password')

    db_b = @client[TEST_DB + '_b']
    silently { db_b.add_user('user_b', 'password') }
    assert db_b.authenticate('user_b', 'password')

    db_c = @client[TEST_DB + '_c']
    silently { db_c.add_user('user_c', 'password') }
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
    teardown_basic
  end

  def test_default_roles_non_admin
    return unless @client.server_version >= '2.5.3'
    init_auth_basic
    silently { @db.add_user('user', 'pass') }
    silently { @db.authenticate('user', 'pass') }
    info = @db.command(:usersInfo => 'user')['users'].first
    assert_equal 'dbOwner', info['roles'].first['role']

    # read-only
    silently { @db.add_user('ro-user', 'pass', true) }
    @db.logout
    @db.authenticate('ro-user', 'pass')
    info = @db.command(:usersInfo => 'ro-user')['users'].first
    assert_equal 'read', info['roles'].first['role']
    @db.logout
    teardown_basic
  end

  def test_delegated_authentication
    return unless @client.server_version >= '2.4' && @client.server_version < '2.5'
    with_auth(@client) do
      init_auth_basic
      # create user in test databases
      accounts = @client[TEST_DB + '_accounts']
      silently do
        accounts.add_user('debbie', 'delegate')
        @db.add_user('debbie', nil, nil, :roles => ['read'], :userSource => accounts.name)
      end
      @admin.logout

      # validate that direct authentication is not allowed
      assert_raise Mongo::AuthenticationError do
        @db.authenticate('debbie', 'delegate')
      end

      # validate delegated authentication
      assert accounts.authenticate('debbie', 'delegate')
      assert @db.collection_names
      accounts.logout
      assert_raise Mongo::OperationFailure do
        @db.collection_names
      end

      # validate auth using source database
      @db.authenticate('debbie', 'delegate', nil, accounts.name)
      assert @db.collection_names
      accounts.logout
      assert_raise Mongo::OperationFailure do
        @db.collection_names
      end

      # clean-up
      @admin.authenticate('admin', 'password')
      remove_all_users(accounts, 'debbie', 'delegate')
      teardown_basic
    end
  end

  def test_non_admin_default_roles
    return if @client.server_version < '2.5'
    init_auth_basic

    # add read-only user and verify that role is 'read'
    @db.add_user('randy', 'password', nil, :roles => ['read'])
    @db.authenticate('randy', 'password')
    users = @db.command(:usersInfo => 'randy')['users']
    assert_equal 'read', users.first['roles'].first['role']
    @db.logout

    # add dbOwner (default) user and verify role
    silently { @db.add_user('emily', 'password') }
    @db.authenticate('emily', 'password')
    users = @db.command(:usersInfo => 'emily')['users']
    assert_equal 'dbOwner', users.first['roles'].first['role']
    teardown_basic
  end

  def test_update_user_to_read_only
    with_auth(@client) do
      init_auth_basic
      silently { @db.add_user('emily', 'password') }
      @admin.logout
      @db.authenticate('emily', 'password')
      @db['test'].insert({})
      @db.logout

      @admin.authenticate('admin', 'password')
      silently { @db.add_user('emily', 'password', true) }
      @admin.logout

      silently { @db.authenticate('emily', 'password') }
      assert_raise Mongo::OperationFailure do
        @db['test'].insert({})
      end
      @db.logout
      @admin.authenticate('admin', 'password')
      teardown_basic
    end
  end

end
