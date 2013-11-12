# Copyright (C) 2013 MongoDB, Inc.
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
      # enable authentication by creating and logging in as admin user
      @admin = @client['admin']
      @admin.add_user('admin', 'password')
      @admin.authenticate('admin', 'password')
      # create a user who can remove all others in teardown
      @db.add_user('admin', 'password')
    end
  end

  def teardown
    remove_all_users(@admin)
    remove_all_users(@db)
  end

  def remove_all_users(db)
    db.logout
    db.authenticate('admin', 'password')
    if @client.server_version < '2.5'
      db['system.users'].remove
    else
      db.command(:dropUsersFromDatabase => 1)
    end
    db.logout
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
    silently do
      return if @client.server_version < '2.5'

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
    db_a = @client['test_a']
    db_a.add_user('user_a', 'password')
    assert db_a.authenticate('user_a', 'password')

    db_b = @client['test_b']
    db_b.add_user('user_b', 'password')
    assert db_b.authenticate('user_b', 'password')

    db_c = @client['test_c']
    db_c.add_user('user_c', 'password')
    assert db_c.authenticate('user_c', 'password')

    socket = @client.checkout_reader(:mode => :primary)
    assert_equal 4, socket.auths.size
    assert_equal @client.auths, socket.auths
    @client.checkin(socket)

    assert db_b.logout
    socket = @client.checkout_reader(:mode => :primary)
    assert_equal 3, socket.auths.size
    assert_equal @client.auths, socket.auths
    @client.checkin(socket)
  end

  #def test_delegated_authentication
  #  return unless @client.server_version >= '2.4' && @client.server_version < '2.5'
#
  #  # TODO: remove this line when slaves have this code:
  #  # https://github.com/travis-ci/travis-cookbooks/pull/180
  #  return if ENV['TRAVIS']
#
  #  doc = {'_id' => 'test'}
  #  # create accounts database to hold user credentials
  #  accounts = @client['accounts']
  #  accounts['system.users'].remove
  #  accounts.add_user('tyler', 'brock', nil, :roles => [])
#
  #  # insert test data and give user permissions on test db
  #  @db['test'].remove
  #  @db['test'].insert(doc)
  #  @db.add_user('tyler', nil, nil, :roles => ['read'], :userSource => 'accounts')
  #  @admin.logout
#
  #  # auth must occur on the db where the user is defined
  #  assert_raise Mongo::AuthenticationError do
  #    @db.authenticate('tyler', 'brock')
  #  end
#
  #  # auth directly
  #  assert accounts.authenticate('tyler', 'brock')
  #  assert_equal doc, @db['test'].find_one
  #  accounts.logout
  #  assert_raise Mongo::OperationFailure do
  #    @db['test'].find_one
  #  end
#
  #  # auth using source
  #  @db.authenticate('tyler', 'brock', true, 'accounts')
  #  assert_equal doc, @db['test'].find_one
  #  @db.logout
  #  assert_raise Mongo::OperationFailure do
  #    @db['test'].find_one
  #  end
#
  #  @db.authenticate('tyler', 'brock', true, 'accounts')
  #  @db.remove_user('tyler')
  #  @db.logout
  #end
end
