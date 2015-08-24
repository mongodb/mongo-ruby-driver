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
  class Collection
    class View

      # Defines iteration related behaviour for collection views, including
      # cursor instantiation.
      #
      # @since 2.0.0
      module Iterable

        # Iterate through documents returned by a query with this +View+.
        #
        # @example Iterate through the result of the view.
        #   view.each do |document|
        #     p document
        #   end
        #
        # @return [ Enumerator ] The enumerator.
        #
        # @since 2.0.0
        #
        # @yieldparam [ Hash ] Each matching document.
        def each
          @cursor = nil
          read_with_retry do
            server = read.select_server(cluster)
            result = send_initial_query(server)
            @cursor = Cursor.new(view, result, server)
          end
          @cursor.each do |doc|
            yield doc
          end if block_given?
          @cursor.to_enum
        end

        # Stop the iteration by sending a KillCursors command to the server.
        #
        # @example Stop the iteration.
        #   view.close_query
        #
        # @since 2.1.0
        def close_query
          @cursor.send(:kill_cursors) if @cursor
        end
      end
    end
  end
end
