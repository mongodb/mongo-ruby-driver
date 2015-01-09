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

module Mongo
  class Database

    # A class representing a view of a database.
    #
    # @since 2.0.0
    class View
      extend Forwardable
      include Enumerable

      def_delegators :@database, :cluster, :server_preference
      def_delegators :cluster, :next_primary

      # @return [ Integer ] batch_size The size of the batch of results
      #   when sending the listCollections command.
      attr_reader :batch_size

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
        server = next_primary
        collections_info(server).collect do |info|
          server.context.features.list_collections_enabled? ?
            info['name'] : info['name'].sub("#{@database.name}.", '')
        end
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
        @cmd_collection = @database['$cmd']
        @batch_size =  nil
      end

      private

      def limit
        -1
      end

      def collection
        @cmd_collection
      end

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
        Operation::Read::CollectionsInfo.new(collections_info_spec)
      end

      def send_initial_query(server)
        initial_query_op.execute(server.context)
      end
    end
  end
end
