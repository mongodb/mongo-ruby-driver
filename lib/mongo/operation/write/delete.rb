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

require 'mongo/operation/write/delete/result'

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
      #     :delete => { :q => { :foo => 1 }, :limit => 1 },
      #     :db_name => 'test',
      #     :coll_name => 'test_coll',
      #     :write_concern => write_concern
      #   })
      #
      # Initialization:
      #   param [ Hash ] spec The specifications for the delete.
      #
      #   option spec :delete [ Hash ] The delete document.
      #   option spec :db_name [ String ] The name of the database on which
      #     the delete should be executed.
      #   option spec :coll_name [ String ] The name of the collection on which
      #     the delete should be executed.
      #   option spec :write_concern [ Mongo::WriteConcern ] The write concern
      #     for this operation.
      #   option spec :ordered [ true, false ] Whether the operations should be
      #     executed in order.
      #   option spec :options [Hash] Options for the command, if it ends up being a
      #     write command.
      #
      # @since 2.0.0
      class Delete
        include Executable
        include Specifiable

        # Execute the delete operation.
        #
        # @example Execute the operation.
        #   operation.execute(context)
        #
        # @param [ Mongo::Server::Context ] context The context for this operation.
        #
        # @return [ Result ] The result.
        #
        # @since 2.0.0
        def execute(context)
          if context.features.write_command_enabled?
            execute_write_command(context)
          else
            execute_message(context)
          end
        end

        private

        def execute_write_command(context)
          s = spec.merge(:deletes => [ delete ])
          s.delete(:delete)
          Result.new(Command::Delete.new(s).execute(context)).validate!
        end

        def execute_message(context)
          context.with_connection do |connection|
            Result.new(connection.dispatch([ message, gle ].compact)).validate!
          end
        end

        def message
          selector = delete[:q]
          opts     = ( delete[:limit] || 0 ) <= 0 ? {} : { :flags => [ :single_remove ] }
          Protocol::Delete.new(db_name, coll_name, selector, opts)
        end
      end
    end
  end
end
