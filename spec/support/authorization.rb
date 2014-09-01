# Copyright (C) 2009-2014 MongoDB, Inc.
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

module Authorization

  # On inclusion provides helpers for use with testing with and without
  # authorization.
  #
  #
  # @since 2.0.0
  def self.included(context)

    # Gets the root system administrator user.
    #
    # @since 2.0.0
    context.let!(:root_user) do
      Mongo::Auth::User.new(
        database: Mongo::Database::ADMIN,
        user: 'root-user',
        password: 'password',
        roles: [
          Mongo::Auth::Roles::USER_ADMIN_ANY_DATABASE,
          Mongo::Auth::Roles::DATABASE_ADMIN_ANY_DATABASE,
          Mongo::Auth::Roles::READ_WRITE_ANY_DATABASE
        ]
      )
    end

    # Get the default test user for the suite.
    #
    # @since 2.0.0
    context.let!(:test_user) do
      Mongo::Auth::User.new(
        database: TEST_DB,
        user: 'test-user',
        password: 'password',
        roles: [
          { role: Mongo::Auth::Roles::READ_WRITE, db: TEST_DB },
          { role: Mongo::Auth::Roles::READ_WRITE, db: TEST_CREATE_DB },
          { role: Mongo::Auth::Roles::READ_WRITE, db: TEST_DROP_DB },
          { role: Mongo::Auth::Roles::DATABASE_ADMIN, db: TEST_DROP_DB }
        ]
      )
    end

    # Provides an authorized mongo client on the default test database for the
    # default test user.
    #
    # @since 2.0.0
    context.let!(:authorized_client) do
      Mongo::Client.new(
        [ '127.0.0.1:27017' ],
        database: TEST_DB,
        user: test_user.name,
        password: test_user.password,
        pool_size: 1
      ).tap do |client|
        client.cluster.scan!
      end
    end

    # Provides an authorized mongo client on the default test database for the
    # default root system administrator.
    #
    # @since 2.0.0
    context.let!(:root_authorized_client) do
      Mongo::Client.new(
        [ '127.0.0.1:27017' ],
        database: TEST_DB,
        user: root_user.name,
        password: root_user.password,
        pool_size: 1
      ).tap do |client|
        client.cluster.scan!
      end
    end

    # Provides an unauthorized mongo client on the default test database.
    #
    # @since 2.0.0
    context.let!(:unauthorized_client) do
      Mongo::Client.new([ '127.0.0.1:27017' ], database: TEST_DB).tap do |client|
        client.cluster.scan!
      end
    end

    # Provides an unauthorized mongo client on the admin database, for use in
    # setting up the first admin root user.
    #
    # @since 2.0.0
    context.let!(:admin_unauthorized_client) do
      Mongo::Client.new([ '127.0.0.1:27017' ], database: Mongo::Database::ADMIN).tap do |client|
        client.cluster.scan!
      end
    end

    # Get an authorized client on the admin database logged in as the admin
    # root user.
    #
    # @since 2.0.0
    context.let!(:admin_authorized_client) do
      admin_unauthorized_client.with(user: root_user.name, password: root_user.password).tap do |client|
        client.cluster.scan!
      end
    end

    # Gets the default test collection from the authorized client.
    #
    # @since 2.0.0
    context.let(:authorized_collection) do
      authorized_client[TEST_COLL]
    end

    # Gets the default test collection from the unauthorized client.
    #
    # @since 2.0.0
    context.let(:unauthorized_collection) do
      unauthorized_client[TEST_COLL]
    end

    # Gets a primary server for the default authorized client.
    #
    # @since 2.0.0
    context.let(:authorized_primary) do
      authorized_client.cluster.servers.first
    end

    # Get a primary server for the client authorized as the root system
    # administrator.
    #
    # @since 2.0.0
    context.let(:root_authorized_primary) do
      root_authorized_client.cluster.servers.first
    end

    # Get a primary server from the unauthorized client.
    #
    # @since 2.0.0
    context.let(:unauthorized_primary) do
      authorized_client.cluster.servers.first
    end

    # Before the suite runs, we create the root admin user and a test user that
    # can read/write/admin our test databases.
    #
    # @since 2.0.0
    context.before(:suite) do
      begin
        admin_unauthorized_client.database.users.create(root_user)
      rescue Exception => e
        p e
      end
      begin
        p admin_authorized_client.database.users.create(test_user)
      rescue Exception => e
        p e
      end
    end
  end
end
