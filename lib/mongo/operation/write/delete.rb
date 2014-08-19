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

require 'mongo/operation/write/delete/response'

module Mongo
  module Operation
    module Write

      # A MongoDB delete operation.
      # If a server with version >= 2.5.5 is selected, a write command operation
      # will be created and sent instead.
      # See Mongo::Operation::Write::Command::Delete
      #
      # @since 2.0.0
      class Delete
        include Executable
        include Slicable

        # Initialize the delete operation.
        #
        # @example
        #   include Mongo
        #   include Operation
        #   Write::Delete.new({ :deletes       => [{ :q => { :foo => 1 },
        #                                            :limit => 1 }],
        #                       :db_name       => 'test',
        #                       :coll_name     => 'test_coll',
        #                       :write_concern => write_concern
        #                     })
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
        # @option spec :opts [Hash] Options for the command, if it ends up being a
        #   write command.
        #
        # @since 2.0.0
        def initialize(spec)
          @spec = spec
        end

        # Execute the operation.
        # If the server has version < 2.5.5, a delete operation is sent.
        # If the server version is >= 2.5.5, a delete write command operation is created
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
            op = Command::Delete.new(spec)
            Response.new(op.execute(context)).verify!
          else
            Response.new(nil, deletes.reduce(0) do |count, d|
              context.with_connection do |connection|
                response = Response.new(connection.dispatch([ message(d), gle ].compact)).verify!
                count + response.n
              end
            end)
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

        # The spec array element to split up when slicing this operation.
        # This is used by the Slicable module.
        #
        # @return [ Symbol ] :deletes
        def slicable_key
          :deletes
        end

        # Dup the list of deletes in the spec if this operation is copied/duped.
        def initialize_copy(original)
          @spec = original.spec.dup
          @spec[:deletes] = original.spec[:deletes].clone
        end

        # The delete documents.
        #
        # @return [ Array ] The delete documents.
        #
        # @since 2.0.0
        def deletes
          @spec[:deletes]
        end

        # The wire protocol message for this delete operation.
        #
        # @return [ Mongo::Protocol::Delete ] Wire protocol message.
        #
        # @since 2.0.0
        def message(delete_spec)
          selector    = delete_spec[:q]
          delete_opts = (delete_spec[:limit] || 0) <= 0 ? {} : { :flags => [:single_remove] }
          Protocol::Delete.new(db_name, coll_name, selector, delete_opts)
        end
      end
    end
  end
end
