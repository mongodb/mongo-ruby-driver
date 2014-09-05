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

# The default test database for all specs.
#
# @since 2.0.0
TEST_DB = 'ruby-driver'.freeze

# The default test collection.
#
# @since 2.0.0
TEST_COLL = 'test'.freeze

# Gets the root system administrator user.
#
# @since 2.0.0
ROOT_USER = Mongo::Auth::User.new(
  database: Mongo::Database::ADMIN,
  user: 'root-user',
  password: 'password',
  roles: [
    Mongo::Auth::Roles::USER_ADMIN_ANY_DATABASE,
    Mongo::Auth::Roles::DATABASE_ADMIN_ANY_DATABASE,
    Mongo::Auth::Roles::READ_WRITE_ANY_DATABASE
  ]
)

# Get the default test user for the suite on versions 2.6 and higher.
#
# @since 2.0.0
TEST_USER = Mongo::Auth::User.new(
  database: Mongo::Database::ADMIN,
  user: 'test-user',
  password: 'password',
  roles: [
    { role: Mongo::Auth::Roles::READ_WRITE, db: TEST_DB },
    { role: Mongo::Auth::Roles::DATABASE_ADMIN, db: TEST_DB }
  ]
)

# MongoDB 2.4 and lower does not allow hashes as roles, so we need to create a
# user on those versions for each database permission in order to ensure the
# legacy roles work with users. The following users are those.

# Gets the default test user for the suite on 2.4 and lower.
#
# @since 2.0.
TEST_READ_WRITE_USER = Mongo::Auth::User.new(
  database: TEST_DB,
  user: 'test-user',
  password: 'password',
  roles: [ Mongo::Auth::Roles::READ_WRITE, Mongo::Auth::Roles::DATABASE_ADMIN ]
)

# Provides an authorized mongo client on the default test database for the
# default test user.
#
# @since 2.0.0
AUTHORIZED_CLIENT = Mongo::Client.new(
  [ '127.0.0.1:27017' ],
  database: TEST_DB,
  user: TEST_USER.name,
  password: TEST_USER.password,
  pool_size: 1
).tap do |client|
  client.cluster.scan!
end

# Provides an authorized mongo client on the default test database for the
# default root system administrator.
#
# @since 2.0.0
ROOT_AUTHORIZED_CLIENT = Mongo::Client.new(
  [ '127.0.0.1:27017' ],
  auth_source: Mongo::Database::ADMIN,
  database: TEST_DB,
  user: ROOT_USER.name,
  password: ROOT_USER.password,
  pool_size: 1
).tap do |client|
  client.cluster.scan!
end

# Provides an unauthorized mongo client on the default test database.
#
# @since 2.0.0
UNAUTHORIZED_CLIENT = Mongo::Client.new(
  [ '127.0.0.1:27017' ],
  database: TEST_DB,
  pool_size: 1
).tap do |client|
  client.cluster.scan!
end

# Provides an unauthorized mongo client on the admin database, for use in
# setting up the first admin root user.
#
# @since 2.0.0
ADMIN_UNAUTHORIZED_CLIENT = Mongo::Client.new(
  [ '127.0.0.1:27017' ],
  database: Mongo::Database::ADMIN,
  pool_size: 1
).tap do |client|
  client.cluster.scan!
end

# Get an authorized client on the admin database logged in as the admin
# root user.
#
# @since 2.0.0
ADMIN_AUTHORIZED_CLIENT = ADMIN_UNAUTHORIZED_CLIENT.with(
  user: ROOT_USER.name,
  password: ROOT_USER.password
).tap do |client|
  client.cluster.scan!
end

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
    context.let(:root_user) { ROOT_USER }

    # Get the default test user for the suite.
    #
    # @since 2.0.0
    context.let(:test_user) { TEST_USER }

    # Provides an authorized mongo client on the default test database for the
    # default test user.
    #
    # @since 2.0.0
    context.let(:authorized_client) { AUTHORIZED_CLIENT }

    # Provides an authorized mongo client on the default test database for the
    # default root system administrator.
    #
    # @since 2.0.0
    context.let(:root_authorized_client) { ROOT_AUTHORIZED_CLIENT }

    # Provides an unauthorized mongo client on the default test database.
    #
    # @since 2.0.0
    context.let!(:unauthorized_client) { UNAUTHORIZED_CLIENT }

    # Provides an unauthorized mongo client on the admin database, for use in
    # setting up the first admin root user.
    #
    # @since 2.0.0
    context.let!(:admin_unauthorized_client) { ADMIN_UNAUTHORIZED_CLIENT }

    # Get an authorized client on the admin database logged in as the admin
    # root user.
    #
    # @since 2.0.0
    context.let!(:admin_authorized_client) { ADMIN_AUTHORIZED_CLIENT }

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
  end
end
