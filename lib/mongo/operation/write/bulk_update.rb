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

require 'mongo/operation/bulk_update/result'

module Mongo
  module Operation
    module Write

      # A MongoDB bulk update operation.
      #
      # @note If the server version is >= 2.5.5, a write command operation
      #   will be created and sent instead.
      #
      # @example Create the update operation.
      #   Write::BulkUpdate.new({
      #     :updates => [
      #       {
      #         :q => { :foo => 1 },
      #         :u => { :$set => { :bar => 1 }},
      #         :multi  => true,
      #         :upsert => false
      #       }
      #     ],
      #     :db_name => 'test',
      #     :coll_name => 'test_coll',
      #     :write_concern => write_concern,
      #     :ordered => false
      #   })
      #
      # @param [ Hash ] spec The specifications for the update.
      #
      # @option spec :updates [ Array ] The update documents.
      # @option spec :db_name [ String ] The name of the database on which
      #   the query should be run.
      # @option spec :coll_name [ String ] The name of the collection on which
      #   the query should be run.
      # @option spec :write_concern [ Mongo::WriteConcern::Mode ] The write concern.
      # @option spec :ordered [ true, false ] Whether the operations should be
      #   executed in order.
      # @option spec :options [ Hash ] Options for the command, if it ends up being a
      #   write command.
      #
      # @since 2.0.0
      class BulkUpdate
        include Executable
        include Specifiable
        include Batchable

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

        # Set the write concern on this operation.
        #
        # @example Set a write concern.
        #   new_op = operation.write_concern(Mongo::WriteConcern::Mode.get(:w => 2))
        #
        # @params [ Mongo::WriteConcern::Mode ] The write concern.
        #
        # @since 2.0.0
        def write_concern(wc = nil)
          if wc
            self.class.new(spec.merge(write_concern: wc))
          else
            spec[WRITE_CONCERN]
          end
        end

        private

        def execute_write_command(context)
          Result.new(Command::Update.new(spec).execute(context))
        end

        def execute_message(context)
          replies = messages(context).map do |m|
            context.with_connection do |connection|
              result = LegacyResult.new(connection.dispatch([ m, gle ].compact))
              if stop_sending?(result)
                return result
              else
                result.reply
              end
            end
          end
          LegacyResult.new(replies.compact.empty? ? nil : replies)
        end

        def stop_sending?(result)
          ordered? && result.write_failure?
        end

        # @todo put this somewhere else
        def ordered?
          @spec.fetch(:ordered, true)
        end

        def gle
          gle_message = ( ordered? && write_concern.get_last_error.nil? ) ?
                           Mongo::WriteConcern::Mode.get(:w => 1).get_last_error :
                           write_concern.get_last_error
          if gle_message
            Protocol::Query.new(
              db_name,
              Database::COMMAND,
              gle_message,
              options.merge(limit: -1)
            )
          end
        end

        def batch_key
          UPDATES
        end

        def initialize_copy(original)
          @spec = original.spec.dup
          @spec[UPDATES] = original.spec[UPDATES].dup
        end

        def messages(context)
          # @todo: break up into multiple messages depending on max_message_size
          updates.collect do |u|
            opts = { :flags => [] }
            opts[:flags] << :multi_update if !!u[:multi]
            opts[:flags] << :upsert if !!u[:upsert]
            Protocol::Update.new(db_name, coll_name, u[:q], u[:u], opts)
            # @todo raise exception if message size exceeds context.max_message_size
          end
        end
      end
    end
  end
end
