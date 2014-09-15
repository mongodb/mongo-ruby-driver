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

      # A MongoDB bulk insert operation.
      # This class should only be used by the Bulk API.
      #
      # @since 2.0.0
      class BulkInsert
        include Executable

        # Initialize the insert operation.
        #
        # @example
        #   include Mongo
        #   include Operation
        #   Write::BulkInsert.new({ :documents     => [{ :foo => 1 }],
        #                           :db_name       => 'test',
        #                           :coll_name     => 'test_coll',
        #                           :write_concern => write_concern
        #                           :ordered       => true
        #                         })
        #
        # @param [ Hash ] spec The specifications for the insert.
        #
        # @option spec :documents [ Array ] The documents to insert.
        # @option spec :db_name [ String ] The name of the database.
        # @option spec :coll_name [ String ] The name of the collection.
        # @option spec :write_concern [ Mongo::WriteConcern::Mode ] The write concern.
        # @option spec :ordered [ true, false ] Whether the operations should be
        #   executed in order and whether the server should abort after the first error.
        # @option spec :opts [ Hash ] Options for the command, if it ends up being a
        #   write command.
        #
        # @since 2.0.0
        def initialize(spec)
          @spec = spec
        end

        # Execute the operation.
        # If the server has version < 2.5.5, an insert operation is sent.
        # If the server version is >= 2.5.5, an insert write command operation is created
        # and sent instead.
        #
        # @params [ Mongo::Server::Context ] The context for this operation.
        #
        # @return [ Mongo::Response ] The operation response, if there is one.
        #
        # @since 2.0.0
        def execute(context)
          if context.write_command_enabled?
            op = Command::Insert.new(spec)
            Result.new(op.execute(context)).validate!
          else
            replies = messages(context).map do |message|
              context.with_connection do |connection|
                # @todo: only validate if it's ordered
                Result.new(connection.dispatch([ message, gle ])).validate!.reply
              end
            end
            Result.new(replies)
          end
        end

        private

        # The get last error command as a wire protocol query.
        # Always use GLE if the bulk operations are ordered.
        #
        # @return [ Protocol::Query ] The GLE command.
        #
        # @since 2.0.0
        def write_concern
          return WriteConcern::Mode.get(WriteConcern::Mode::DEFAULT) if ordered?
          @spec[:write_concern]
        end

        # Dup the list of documents in the spec if this operation is copied/duped.
        def initialize_copy(original)
          @spec = original.spec.dup
          @spec[:documents] = original.spec[:documents].dup
        end

        # The documents to insert.
        #
        # @return [ Array ] The documents.
        #
        # @since 2.0.0
        def documents
          @spec[:documents]
        end

        def ordered?
          !!@spec[:ordered]
        end

        # The wire protocol message for this insert operation.
        #
        # @params [ Mongo::Server::Context ] The context to use for this operation.
        #
        # @return [ Array ] Wire protocol message(s).
        #
        # @since 2.0.0
        def messages(context)
          if ordered?
            documents.collect do |doc|
              Protocol::Insert.new(db_name, coll_name, doc)
            end
          else
            [ Protocol::Insert.new(db_name, coll_name, documents, { :flags => [:continue_on_error] }) ]
            # @todo: check size after serialization and split if it's too large.
          end
        end
      end
    end
  end
end
