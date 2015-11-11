# Copyright (C) 2014-2015 MongoDB, Inc.
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
  class Database

    # A class representing a view of a database.
    #
    # @since 2.0.0
    class View
      extend Forwardable
      include Enumerable

      def_delegators :@database, :cluster, :read_preference
      def_delegators :cluster, :next_primary

      # @return [ Integer ] batch_size The size of the batch of results
      #   when sending the listCollections command.
      attr_reader :batch_size

      # @return [ Integer ] limit The limit when sending a command.
      attr_reader :limit

      # @return [ Collection ] collection The command collection.
      attr_reader :collection

      # Get all the names of the non system collections in the database.
      #
      # @example Get the collection names.
      #   database.collection_names
      #
      # @param [ Hash ] options Options for the listCollections command.
      #
      # @option options [ Integer ] :batch_size  The batch size for results
      #   returned from the listCollections command.
      #
      # @return [ Array<String> ] The names of all non-system collections.
      #
      # @since 2.0.0
      def collection_names(options = {})
        @batch_size = options[:batch_size]
        server = next_primary(false)
        @limit = -1 if server.features.list_collections_enabled?
        collections_info(server).collect do |info|
          if server.features.list_collections_enabled?
            info[Database::NAME]
          else
            (info[Database::NAME] &&
              info[Database::NAME].sub("#{@database.name}.", ''))
          end
        end
      end

      # Get info on all the collections in the database.
      #
      # @example Get info on each collection.
      #   database.list_collections
      #
      # @return [ Array<Hash> ] Info for each collection in the database.
      #
      # @since 2.0.5
      def list_collections
        collections_info(next_primary(false))
      end

      # Create the new database view.
      #
      # @example Create the new database view.
      #   View::Index.new(database)
      #
      # @param [ Database ] database The database.
      #
      # @since 2.0.0
      def initialize(database)
        @database = database
        @batch_size =  nil
        @limit = nil
        @collection = @database[Database::COMMAND]
      end

      private

      def collections_info(server, &block)
        cursor = Cursor.new(self, send_initial_query(server), server).to_enum
        cursor.each do |doc|
          yield doc
        end if block_given?
        cursor
      end

      def collections_info_spec
        { selector: {
            listCollections: 1,
            cursor: batch_size ? { batchSize: batch_size } : {} },
          db_name: @database.name }
      end

      def initial_query_op
        Operation::Commands::CollectionsInfo.new(collections_info_spec)
      end

      def send_initial_query(server)
        initial_query_op.execute(server.context)
      end
    end
  end
end
