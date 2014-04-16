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

  module Operation

    module Read

      # A MongoDB query operation with context describing
      # what server or socket it should be sent to.
      #
      # @since 3.0.0
      class Query
        include Executable

        # The server preference for this query operation.
        # In other words, the "read preference".
        #
        # @return [ Object ] The server preference.
        #
        # @since 3.0.0
        attr_reader :server_preference

        # Initialize the query operation.
        #
        # @example Initialize a query operation.
        #   secondary_preference = Mongo::ServerPreference.get(:secondary)
        #   Mongo::Operation::Query.new({ :selector => { :foo => 1 } },
        #                               { :server_preference => secondary_preference })
        #
        # @param [ Hash ] spec The specifications for the query.
        # @param [ Hash ] context The context for executing this operation.
        #
        # @option spec :selector [ Hash ] The query selector.
        # @option spec :db_name [ String ] The name of the database on which
        #   the query should be run.
        # @option spec :coll_name [ String ] The name of the collection on which
        #   the query should be run.
        # @option spec :opts [ Hash ] Options for the query.
        #
        # @option context :server_preference [ Mongo::ServerPreference ] The server
        #   preference for where the operation should be sent.
        # @option context :server [ Mongo::Server ] The server that the operation
        #   should be sent to.
        #
        # @since 3.0.0
        def initialize(spec, context={})
          @spec              = spec

          @server_preference = context[:server_preference]
          @server            = context[:server]
        end

        private

        # The selector for the query.
        #
        # @return [ Hash ] The query selector. 
        #
        # @since 3.0.0
        def selector
          @spec[:selector]
        end

        # The options for the query.
        #
        # @return [ Hash ] The query options.
        #
        # @since 3.0.0
        def query_opts
          @spec[:opts] || {}
        end

        # The wire protocol message for this query operation.
        #
        # @return [ Mongo::Protocol::Query ] Wire protocol message.
        #
        # @since 3.0.0
        def message
          Mongo::Protocol::Query.new(db_name, coll_name, selector, query_opts)
        end
      end
    end
  end
end
