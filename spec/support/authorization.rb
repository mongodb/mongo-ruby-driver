# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2009-2020 MongoDB Inc.
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

# The default test collection.
#
# @since 2.0.0
TEST_COLL = 'test'.freeze

# An invalid write concern.
#
# @since 2.4.2
INVALID_WRITE_CONCERN = { w: 4000 }

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
    context.let(:root_user) { SpecConfig.instance.root_user }

    # Get the default test user for the suite.
    #
    # @since 2.0.0
    context.let(:test_user) { SpecConfig.instance.test_user }

    # Provides an authorized mongo client on the default test database for the
    # default test user.
    #
    # @since 2.0.0
    context.let(:authorized_client) { ClientRegistry.instance.global_client('authorized') }

    # A client with a different cluster, for testing session use across
    # clients
    context.let(:another_authorized_client) do
      new_local_client(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options.merge(
          database: SpecConfig.instance.test_db,
          user: SpecConfig.instance.test_user.name,
          password: SpecConfig.instance.test_user.password,
          heartbeat_frequency: 10,
        ),
      )
    end

    # Provides an authorized mongo client on the default test database that retries writes.
    #
    # @since 2.5.1
    context.let(:authorized_client_with_retry_writes) do
      ClientRegistry.instance.global_client('authorized_with_retry_writes')
    end

    context.let(:authorized_client_without_retry_writes) do
      ClientRegistry.instance.global_client('authorized_without_retry_writes')
    end

    context.let(:authorized_client_without_retry_reads) do
      ClientRegistry.instance.global_client('authorized_without_retry_reads')
    end

    context.let(:authorized_client_without_any_retry_reads) do
      ClientRegistry.instance.global_client('authorized_without_any_retry_reads')
    end

    context.let(:authorized_client_without_any_retries) do
      ClientRegistry.instance.global_client('authorized_without_any_retries')
    end

    # Provides an unauthorized mongo client on the default test database.
    #
    # @since 2.0.0
    context.let(:unauthorized_client) { ClientRegistry.instance.global_client('unauthorized') }

    # Provides an unauthorized mongo client on the admin database, for use in
    # setting up the first admin root user.
    #
    # @since 2.0.0
    context.let(:admin_unauthorized_client) { ClientRegistry.instance.global_client('admin_unauthorized') }

    # Get an authorized client on the test database logged in as the admin
    # root user.
    #
    # @since 2.0.0
    context.let(:root_authorized_client) { ClientRegistry.instance.global_client('root_authorized') }

    context.let(:root_authorized_admin_client) do
      ClientRegistry.instance.global_client('root_authorized').use(:admin)
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
      authorized_client.cluster.next_primary
    end

    # Get a primary server for the client authorized as the root system
    # administrator.
    #
    # @since 2.0.0
    context.let(:root_authorized_primary) do
      root_authorized_client.cluster.next_primary
    end

    # Get a primary server from the unauthorized client.
    #
    # @since 2.0.0
    context.let(:unauthorized_primary) do
      authorized_client.cluster.next_primary
    end

    # Get a default address (of the primary).
    #
    # @since 2.2.6
    context.let(:default_address) do
      authorized_client.cluster.next_primary.address
    end

    # Get a default app metadata.
    #
    # @since 2.4.0
    context.let(:app_metadata) do
      authorized_client.cluster.app_metadata
    end
  end
end
