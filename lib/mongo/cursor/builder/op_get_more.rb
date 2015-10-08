# Copyright (C) 2015 MongoDB, Inc.
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
  class Cursor
    module Builder

      # Encapsulates behaviour around generating an OP_GET_MORE specification.
      #
      # @since 2.2.0
      class OpGetMore
        extend Forwardable

        # @return [ Cursor ] cursor The cursor.
        attr_reader :cursor

        def_delegators :@cursor, :collection_name, :database, :to_return

        # Create the new builder.
        #
        # @example Create the builder.
        #   OpGetMore.new(cursor)
        #
        # @param [ Cursor ] cursor The cursor.
        #
        # @since 2.2.0
        def initialize(cursor)
          @cursor = cursor
        end

        # Get the specification.
        #
        # @example Get the specification.
        #   op_get_more.specification
        #
        # @return [ Hash ] The specification.
        #
        # @since 2.2.0
        def specification
          {
            :to_return => to_return,
            :cursor_id => cursor.id,
            :db_name   => database.name,
            :coll_name => collection_name
          }
        end
      end
    end
  end
end
