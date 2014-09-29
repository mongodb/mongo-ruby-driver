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

      # A MongoDB update operation.
      #
      # @note If the server version is >= 2.5.5, a write command operation
      #   will be created and sent instead.
      #
      # @example Create the update operation.
      #   Write::Update.new({
      #     :update =>
      #       {
      #         :q => { :foo => 1 },
      #         :u => { :$set => { :bar => 1 }},
      #         :multi  => true,
      #         :upsert => false
      #       },
      #     :db_name => 'test',
      #     :coll_name => 'test_coll',
      #     :write_concern => write_concern
      #   })
      #
      # @param [ Hash ] spec The specifications for the update.
      #
      # @option spec :update [ Hash ] The update document.
      # @option spec :db_name [ String ] The name of the database on which
      #   the query should be run.
      # @option spec :coll_name [ String ] The name of the collection on which
      #   the query should be run.
      # @option spec :write_concern [ Mongo::WriteConcern::Mode ] The write concern.
      # @option spec :options [ Hash ] Options for the command, if it ends up being a
      #   write command.
      #
      # @since 2.0.0
      class Update
        include Executable
        include Specifiable

        # Execute the update operation.
        #
        # @example Execute the operation.
        #   operation.execute(context)
        #
        # @params [ Mongo::Server::Context ] The context for this operation.
        #
        # @return [ Result ] The operation result.
        #
        # @since 2.0.0
        def execute(context)
          if context.write_command_enabled?
            execute_write_command(context)
          else
            execute_message(context)
          end
        end

        private

        def execute_write_command(context)
          s = spec.merge(:updates => [ update ])
          s.delete(:update)
          Result.new(Command::Update.new(s).execute(context)).validate!
        end

        def execute_message(context)
          context.with_connection do |connection|
            Result.new(connection.dispatch([ message, gle ].compact)).validate!
          end
        end

        def initialize_copy(original)
          @spec = original.spec.dup
          @spec[:updates] = original.spec[:updates].dup
        end

        def message(update_spec = {})
          opts = update[:multi] ? { :flags => [:multi_update] } : {}
          Protocol::Update.new(db_name, coll_name, update[:q], update[:u], opts)
        end
      end
    end
  end
end