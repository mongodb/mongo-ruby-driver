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

require 'mongo/operation/write/insert/response'

module Mongo
  module Operation
    module Write

      # A MongoDB insert operation.
      # If a server with version >= 2.5.5 is being used, a write command operation
      # will be created and sent instead.
      # See Mongo::Operation::Write::WriteCommand::Insert
      #
      # @since 2.0.0
      class Insert
        include Executable
        include Slicable

        # Initialize the insert operation.
        #
        # @example
        #   include Mongo
        #   include Operation
        #   Write::Insert.new({ :documents     => [{ :foo => 1 }],
        #                       :db_name       => 'test',
        #                       :coll_name     => 'test_coll',
        #                       :write_concern => write_concern
        #                     })
        #
        # @param [ Hash ] spec The specifications for the insert.
        #
        # @option spec :documents [ Array ] The documents to insert.
        # @option spec :db_name [ String ] The name of the database.
        # @option spec :coll_name [ String ] The name of the collection.
        # @option spec :write_concern [ Mongo::WriteConcern::Mode ] The write concern.
        # @option spec :ordered [ true, false ] Whether the operations should be
        #   executed in order.
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
          unless context.primary? || context.standalone?
            raise Exception, "Must use primary server"
          end
          if context.write_command_enabled?
            op = WriteCommand::Insert.new(spec)
            Response.new(op.execute(context)).verify!
          else
            documents.each do |d|
              context.with_connection do |connection|
                Response.new(connection.dispatch([ message(d), gle ].compact)).verify!
              end
            end
            Response.new(nil, documents.size)
          end
        end

        # Merge another insert operation with this one.
        # Requires that the collection and database of the two ops are the same.
        #
        # @params[ Mongo::Operation::Write::Insert ] The other insert operation.
        #
        # @return [ self ] This object with the list of documents merged.
        #
        # @since 2.0.0
        def merge!(other)
          # @todo: use specific exception
          raise Exception, "Cannot merge" unless self.class == other.class &&
              coll_name == other.coll_name &&
              db_name == other.db_name
          @spec[:documents] << other.spec[:documents]
          self
        end

        private

        # The spec array element to split up when slicing this operation.
        # This is used by the Slicable module.
        #
        # @return [ Symbol ] :documents
        def slicable_key
          :documents
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

        # The wire protocol message for this insert operation.
        #
        # @return [ Mongo::Protocol::Insert ] Wire protocol message.
        #
        # @since 2.0.0
        def message(document)
          insert_spec = options[:continue_on_error] == 0 ? {} : { :flags => [:continue_on_error] }
          Protocol::Insert.new(db_name, coll_name, [ document ], insert_spec)
        end
      end
    end
  end
end
