# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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

module Mongo
  module Auth

    # Provides constants for the built in roles provided by MongoDB.
    #
    # @since 2.0.0
    module Roles

      # @see https://www.mongodb.com/docs/manual/reference/built-in-roles/#backup
      #
      # @since 2.0.0
      BACKUP = 'backup'.freeze

      # @see https://www.mongodb.com/docs/manual/reference/built-in-roles/#clusterAdmin
      #
      # @since 2.0.0
      CLUSTER_ADMIN = 'clusterAdmin'.freeze

      # @see https://www.mongodb.com/docs/manual/reference/built-in-roles/#clusterManager
      #
      # @since 2.0.0
      CLUSTER_MANAGER = 'clusterManager'.freeze

      # @see https://www.mongodb.com/docs/manual/reference/built-in-roles/#clusterMonitor
      #
      # @since 2.0.0
      CLUSTER_MONITOR = 'clusterMonitor'.freeze

      # @see https://www.mongodb.com/docs/manual/reference/built-in-roles/#dbAdmin
      #
      # @since 2.0.0
      DATABASE_ADMIN = 'dbAdmin'.freeze

      # @see https://www.mongodb.com/docs/manual/reference/built-in-roles/#dbAdminAnyDatabase
      #
      # @since 2.0.0
      DATABASE_ADMIN_ANY_DATABASE = 'dbAdminAnyDatabase'.freeze

      # @see https://www.mongodb.com/docs/manual/reference/built-in-roles/#dbOwner
      #
      # @since 2.0.0
      DATABASE_OWNER = 'dbOwner'.freeze

      # @see https://www.mongodb.com/docs/manual/reference/built-in-roles/#hostManager
      #
      # @since 2.0.0
      HOST_MANAGER = 'hostManager'.freeze

      # @see https://www.mongodb.com/docs/manual/reference/built-in-roles/#read
      #
      # @since 2.0.0
      READ = 'read'.freeze

      # @see https://www.mongodb.com/docs/manual/reference/built-in-roles/#readAnyDatabase
      #
      # @since 2.0.0
      READ_ANY_DATABASE = 'readAnyDatabase'.freeze

      # @see https://www.mongodb.com/docs/manual/reference/built-in-roles/#readWriteAnyDatabase
      #
      # @since 2.0.0
      READ_WRITE_ANY_DATABASE = 'readWriteAnyDatabase'.freeze

      # @see https://www.mongodb.com/docs/manual/reference/built-in-roles/#readWrite
      #
      # @since 2.0.0
      READ_WRITE = 'readWrite'.freeze

      # @see https://www.mongodb.com/docs/manual/reference/built-in-roles/#restore
      #
      # @since 2.0.0
      RESTORE = 'restore'.freeze

      # @see https://www.mongodb.com/docs/manual/reference/built-in-roles/#root
      #
      # @since 2.0.0
      ROOT = 'root'.freeze

      # @see https://www.mongodb.com/docs/manual/reference/built-in-roles/#userAdmin
      #
      # @since 2.0.0
      USER_ADMIN = 'userAdmin'.freeze

      # @see https://www.mongodb.com/docs/manual/reference/built-in-roles/#userAdminAnyDatabase
      #
      # @since 2.0.0
      USER_ADMIN_ANY_DATABASE = 'userAdminAnyDatabase'.freeze
    end
  end
end
