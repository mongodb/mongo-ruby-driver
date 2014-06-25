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

      # A MongoDB get more operation.
      #
      # @since 3.0.0
      class GetMore
        include Executable

        # Initialize a get more operation.
        #
        # @example
        #   include Mongo
        #   include Operation
        #   Read::GetMore.new(collection, 
        #                     :to_return => 50,
        #                     :cursor_id => 1)
        #
        # @param [ Collection ] collection The collection on which the
        #   get more will be executed.
        # @param [ Hash ] spec The specifications for the operation.
        #
        # @option spec :to_return [ Integer ] The number of results to return.
        # @option spec :cursor_id [ Integer ] The id of the cursor.
        #
        # @since 3.0.0
        def initialize(collection, spec)
          @collection = collection
          @spec       = spec
        end

        private

        # The number of documents to request from the server.
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
        # @return [ Mongo::Protocol::GetMore ] Wire protocol message.
        #
        # @since 3.0.0
        def message
          Mongo::Protocol::GetMore.new(db_name, coll_name, to_return, cursor_id)
        end
      end
    end
  end
end
