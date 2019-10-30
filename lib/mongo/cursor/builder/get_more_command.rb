# Copyright (C) 2015-2019 MongoDB, Inc.
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

      # Generates a specification for a get more command.
      #
      # @since 2.2.0
      class GetMoreCommand
        extend Forwardable

        # @return [ Cursor ] cursor The cursor.
        attr_reader :cursor

        def_delegators :@cursor, :collection_name, :database, :view
        def_delegators :view, :batch_size

        # Create the new builder.
        #
        # @example Create the builder.
        #   GetMoreCommand.new(cursor)
        #
        # @param [ Cursor ] cursor The cursor.
        # @param [ Session ] session The session.
        #
        # @since 2.2.0
        def initialize(cursor, session = nil)
          @cursor = cursor
          @session = session
        end

        # Get the specification.
        #
        # @example Get the specification.
        #   get_more_command.specification
        #
        # @return [ Hash ] The spec.
        #
        # @since 2.2.0
        def specification
          { selector: get_more_command, db_name: database.name, session: @session }
        end

        private

        def get_more_command
          command = {
            :getMore => BSON::Int64.new(cursor.id),
            :collection => collection_name,
          }
          command[:batchSize] = batch_size.abs if batch_size && batch_size != 0
          # If the max_await_time_ms option is set, then we set maxTimeMS on
          # the get more command.
          if view.respond_to?(:max_await_time_ms)
            if view.max_await_time_ms && view.options[:await_data]
              command[:maxTimeMS] = view.max_await_time_ms
            end
          end
          command
        end
      end
    end
  end
end
