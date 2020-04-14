# Copyright (C) 2015-2020 MongoDB Inc.
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

require 'mongo/operation/collections_info/result'

module Mongo
  module Operation

    # A MongoDB operation to get info on all collections in a given database.
    #
    # @api private
    #
    # @since 2.0.0
    class CollectionsInfo
      include Specifiable
      include Executable
      include ReadPreferenceSupported
      include PolymorphicResult

      # Execute the operation.
      #
      # @example
      #   operation.execute(connection, client: nil)
      #
      # @param [ Mongo::Server::Connection ] connection The connection over
      #   which to send the operation.
      # @param [ Mongo::Client ] client The client that will be used to
      #   perform auto-encryption if it is necessary to encrypt the command
      #   being executed (optional).
      #
      # @return [ Mongo::Operation::CollectionsInfo::Result,
      #           Mongo::Operation::ListCollections::Result ] The operation result.
      #
      # @since 2.0.0
      def execute(connection, client:)
        if connection.features.list_collections_enabled?
          return Operation::ListCollections.new(spec).execute(connection, client: client)
        end

        super
      end

      private

      def selector(server)
        {}
      end

      def message(server)
        Protocol::Query.new(db_name, Database::NAMESPACES, command(server), options(server))
      end
    end
  end
end
