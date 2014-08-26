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
    @test_user = 'bob'
    @test_user_pwd = 'user'

    # db user for cleanup (for pre-2.4)
    @db.add_user('admin', 'cleanup', nil, :roles => [])
  end

  def teardown
    @client[TEST_DB].authenticate(TEST_USER, TEST_USER_PWD) unless has_auth?(TEST_DB, TEST_USER)

    if @client.server_version < '2.5'
      @db['system.users'].remove
    else
      @db.command(:dropAllUsersFromDatabase => 1)
    end
  end

  def remove_user(database, username, password)
    database.authenticate(username, password) unless has_auth?(database.name, username)
    database.remove_user(username)
    database.logout
  end

  def has_auth?(db_name, username)
    @client.auths.any? { |a| a[:source] == db_name && a[:username] == username }
  end

  def test_add_remove_user
    init_auth_basic

    # add user
    silently { @db.add_user(@test_user, @test_user_pwd) }
    assert @db.authenticate(@test_user, @test_user_pwd)

    # remove user
    assert @db.remove_user(@test_user)
  end

  def test_update_user
    init_auth_basic

    # add user
    silently { @db.add_user(@test_user, @test_user_pwd) }
    assert @db.authenticate(@test_user, @test_user_pwd)
    @db.logout

    # update user
    silently { @db.add_user(@test_user, 'updated') }
    assert_raise Mongo::AuthenticationError do
      @db.authenticate('bob', 'user')
    end
    assert @db.authenticate('bob', 'updated')
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
  end

  def test_authenticate
    init_auth_basic
    silently { @db.add_user(@test_user, @test_user_pwd) }
    assert @db.authenticate(@test_user, @test_user_pwd)
  end

  def test_authenticate_non_existent_user
    init_auth_basic
    assert_raise Mongo::AuthenticationError do
      @db.authenticate('frank', 'thetank')
    end
  end

  def test_logout
    init_auth_basic
    silently { @db.add_user(@test_user, @test_user_pwd) }
    assert @db.authenticate(@test_user, @test_user_pwd)
    assert @db.logout
  end

  def test_authenticate_with_special_characters
    init_auth_basic
    silently { assert @db.add_user('foo:bar','@foo') }
    assert @db.authenticate('foo:bar','@foo')
  end

  def test_authenticate_read_only
    init_auth_basic
    silently { @db.add_user(@test_user, @test_user_pwd, true) }
    assert @db.authenticate(@test_user, @test_user_pwd)
  end

  def test_authenticate_with_connection_uri
    init_auth_basic
    silently { @db.add_user(@test_user, @test_user_pwd) }

    uri    = "mongodb://#{@test_user}:#{@test_user_pwd}@#{@host_info}/#{@db.name}"
    client = Mongo::URIParser.new(uri).connection

    assert client
    assert_equal client.auths.size, 1
    assert client[@db.name]['auth_test'].count

    auth = client.auths.first
    assert_equal @db.name, auth[:db_name]
    assert_equal @test_user, auth[:username]
    assert_equal @test_user_pwd, auth[:password]
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
    remove_user(db_a, 'user_a', 'password')
    remove_user(db_b, 'user_b', 'password')
    remove_user(db_c, 'user_c', 'password')
  end

  def test_default_roles_non_admin
    init_auth_basic
    @db.stubs(:command).returns({}, true)
    @db.expects(:command).with do |command, cmd_opts|
      command[:createUser] == @test_user
      cmd_opts[:roles] == ['dbOwner'] if cmd_opts
    end

    silently { @db.add_user(@test_user, @test_user_pwd) }
  end

  def test_default_roles_non_admin_read_only
    init_auth_basic
    @db.stubs(:command).returns({}, true)
    @db.expects(:command).with do |command, cmd_opts|
      command[:createUser] == @test_user
      cmd_opts[:roles] == ['read'] if cmd_opts
    end
    silently { @db.add_user(@test_user, @test_user_pwd, true) }
  end

  def test_delegated_authentication
    return unless @client.server_version >= '2.4' && @client.server_version < '2.5'
    with_auth(@client) do
      init_auth_basic
      # create user in test databases
      accounts = @client[TEST_DB + '_accounts']
      silently do
        accounts.add_user('emily', 'password')
        @db.add_user('emily', nil, nil, :roles => ['read'], :userSource => accounts.name)
      end
      @admin.logout

      # validate that direct authentication is not allowed
      assert_raise Mongo::AuthenticationError do
        @db.authenticate('emily', 'password')
      end

      # validate delegated authentication
      assert accounts.authenticate('emily', 'password')
      assert @db.collection_names
      accounts.logout
      assert_raise Mongo::OperationFailure do
        @db.collection_names
      end

      # validate auth using source database
      @db.authenticate('emily', 'password', nil, accounts.name)
      assert @db.collection_names
      accounts.logout
      assert_raise Mongo::OperationFailure do
        @db.collection_names
      end

      remove_user(accounts, 'emily', 'password')
    end
  end

  def test_update_user_to_read_only
    with_auth(@client) do
      init_auth_basic
      silently { @db.add_user(@test_user, @test_user_pwd) }
      @admin.logout
      @db.authenticate(@test_user, @test_user_pwd)
      @db['test'].insert({})
      @db.logout

      @admin.authenticate(TEST_USER, TEST_USER_PWD)
      silently { @db.add_user('emily', 'password', true) }
      @admin.logout

      silently { @db.authenticate('emily', 'password') }
      assert_raise Mongo::OperationFailure do
        @db['test'].insert({})
      end
      @db.logout
      @admin.authenticate(TEST_USER, TEST_USER_PWD)
    end
  end
end
