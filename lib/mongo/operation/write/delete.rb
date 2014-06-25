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
      # If a server with version >= 2.5.5 is selected, a write command operation
      # will be created and sent instead.
      # See Mongo::Operation::Write::WriteCommand::Delete
      #
      # @since 2.0.0
      class Delete
        include Executable

        # Initialize the delete operation.
        #
        # @example
        #   include Mongo
        #   include Operation
        #   Write::Delete.new(collection,
        #                     :deletes       => [{ :q => { :foo => 1 },
        #                                            :limit => 1 }],
        #                       :write_concern => write_concern)
        #
        # @param [ Hash ] spec The specifications for the delete.
        # @param [ Collection ] collection The collection on which the delete will be
        #   executed.
        #
        # @option spec :deletes [ Array ] The delete documents.
        # @option spec :write_concern [ Mongo::WriteConcern::Mode ] The write concern
        #   for this operation.
        # @option spec :opts [Hash] Options for the command, if it ends up being a
        #   write command.
        #
        # @since 2.0.0
        def initialize(collection, spec)
          @collection = collection
          @spec       = spec
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
          raise Exception, "Must use primary server" unless context.primary?
          # @todo: change wire version to constant
          if context.wire_version >= 2
            op = WriteCommand::Delete.new(collection, spec.merge(:delete => coll_name))
            op.execute(context)
          else
            deletes.each do |d|
              context.with_connection do |connection|
                gle = write_concern.get_last_error
                connection.dispatch([message(d), gle].compact)
              end
            end
          end
        end

        private

        # The write concern to use for this operation.
        #
        # @return [ Mongo::WriteConcern::Mode ] The write concern.
        #
        # @since 2.0.0
        def write_concern
          @spec[:write_concern]
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
          delete_opts = delete_spec[:limit] == 0 ? { } : { :flags => [:single_remove] }
          Mongo::Protocol::Delete.new(db_name, coll_name, selector, delete_opts)
        end
      end
    end
  end
end
