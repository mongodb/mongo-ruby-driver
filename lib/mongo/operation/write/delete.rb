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
    module Write

      # A MongoDB delete operation.
      #
      # @note If a server with version >= 2.5.5 is selected, a write command
      #   operation will be created and sent instead.
      #
      # @example Create the delete operation.
      #   Write::Delete.new({
      #     :deletes => [{ :q => { :foo => 1 }, :limit => 1 }],
      #     :db_name => 'test',
      #     :coll_name => 'test_coll',
      #     :write_concern => write_concern
      #   })
      #
      # @param [ Hash ] spec The specifications for the delete.
      #
      # @option spec :deletes [ Array ] The delete documents.
      # @option spec :db_name [ String ] The name of the database on which
      #   the delete should be executed.
      # @option spec :coll_name [ String ] The name of the collection on which
      #   the delete should be executed.
      # @option spec :write_concern [ Mongo::WriteConcern::Mode ] The write concern
      #   for this operation.
      # @option spec :ordered [ true, false ] Whether the operations should be
      #   executed in order.
      # @option spec :options [Hash] Options for the command, if it ends up being a
      #   write command.
      #
      # @since 2.0.0
      class Delete
        include Executable
        include Slicable
        include Specifiable

        # Execute the delete operation.
        #
        # @example Execute the operation.
        #   operation.execute(context)
        #
        # @params [ Mongo::Server::Context ] The context for this operation.
        #
        # @return [ Result ] The result.
        #
        # @since 2.0.0
        def execute(context)
          if context.write_command_enabled?
            execute_write_command(context)
          else
            execute_message(context)
          end
        end

        # Merge another delete operation with this one.
        # Requires that the collection and database of the two ops are the same.
        #
        # @params[ Mongo::Operation::Write::Delete ] The other delete operation.
        #
        # @return [ self ] This object with the list of deletes merged.
        #
        # @since 2.0.0
        def merge!(other)
          # @todo: use specific exception
          raise Exception, "Cannot merge" unless self.class == other.class &&
              coll_name == other.coll_name &&
              db_name == other.db_name
          @spec[:deletes] << other.spec[:deletes]
          self
        end

        private

        def execute_write_command(context)
          Result.new(Command::Delete.new(spec).execute(context)).validate!
        end

        def execute_message(context)
          replies = deletes.map do |d|
            context.with_connection do |connection|
              Result.new(connection.dispatch([ message(d), gle ].compact)).validate!.reply
            end
          end
          Result.new(replies)
        end

        def slicable_key
          :deletes
        end

        def initialize_copy(original)
          @spec = original.spec.dup
          @spec[:deletes] = original.spec[:deletes].clone
        end

        def message(delete_spec)
          selector    = delete_spec[:q]
          delete_options = (delete_spec[:limit] || 0) <= 0 ? {} : { :flags => [:single_remove] }
          Protocol::Delete.new(db_name, coll_name, selector, delete_options)
        end
      end
    end
  end
end
