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

      # A MongoDB get more operation with context describing
      # what server or socket it should be sent to.
      #
      # @since 3.0.0
      class GetMore
        include Executable

        # Initialize a get more operation.
        # Note that a server must always be specified, because by definition,
        # a get more operation requests more results from an existing cursor.
        #
        # @example
        #   include Mongo
        #   include Operation
        #   Read::GetMore.new({ :selector => { :to_return => 50,
        #                                      :cursor_id => 1,
        #                                      :db_name   => 'test_db',
        #                                      :coll_name => 'test_coll' } },
        #                       :server => server )
        #
        # @param [ Hash ] spec The specifications for the operation.
        # @param [ Hash ] context The context for executing this operation.
        #
        # @option spec :selector [ Hash ] The get more selector.
        # @option spec :db_name [ String ] The name of the database on which
        #   the operation should be executed.
        # @option spec :coll_name [ String ] The name of the collection on which
        #   the operation should be executed.
        # @option spec :opts [ Hash ] Options for the map reduce command.
        #
        # @option context :server [ Mongo::Server ] The server to use for the operation.
        # @option context :connection [ Mongo::Socket ] The socket that the operation
        #   message should be sent on.
        #
        # @since 3.0.0
        def initialize(spec, context={})
          # @todo: Replace with appropriate error
          # @todo: can you specify a connection?
          raise Exception, 'You must specify a server' unless @server = context[:server]
          @spec       = spec
          @connection = context[:connection]
        end

        # The context to be used for executing the operation.
        #
        # @return [ Hash ] The context.
        #
        # @since 3.0.0
        def context
          { :server     => @server,
            :connection => @connection }
        end

        private

        # The number of documents requested from the server.
        #
        # @return [ Integer ] The number of documents to return.
        #
        # @since 3.0.0
        def to_return
          @spec[:to_return]
        end

        # The id of the cursor created on the server.
        #
        # @return [ Integer ] The cursor id.
        #
        # @since 3.0.0
        def cursor_id
          @spec[:cursor_id]
        end

        # The wire protocol message for this get more operation.
        #
        # @return [ Mongo::Protocol::Query ] Wire protocol message.
        #
        # @since 3.0.0
        def message
          Mongo::Protocol::GetMore.new(db_name, coll_name, to_return, cursor_id)
        end
      end
    end
  end
end
