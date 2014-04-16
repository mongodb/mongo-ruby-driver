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
      # @since 3.0.0
      class Delete
        include Executable

        # Initialize the delete operation.
        #
        # @example Initialize a delete operation.
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
        # @param [ Hash ] context The context for executing this operation.
        #
        # @option spec :deletes [ Array ] The delete documents.
        # @option spec :db_name [ String ] The name of the database on which
        #   the delete should be executed.
        # @option spec :coll_name [ String ] The name of the collection on which
        #   the delete should be executed.
        # @option spec :write_concern [ Object ] The write concern for this operation.
        # @option spec :ordered [ true, false ] Whether the operations should be
        #   executed in order.
        # @option spec :opts [Hash] Options for the command, if it ends up being a
        #   write command.
        #
        # @option context :server [ Mongo::Server ] The server that the operation
        #   should be sent to.
        #
        # @since 3.0.0
        def initialize(spec, context = {})
          @spec       = spec
          @server     = context[:server]
        end

        # Execute the operation.
        # The client uses the context to get a server. If the server is
        # version < 2.5.5, a delete wire protocol operation is sent.
        # If the server version is >= 2.5.5, a delete write command operation is created
        # and sent instead.
        #
        # @params [ Mongo::Client ] The client to use to get a server.
        #
        # @todo: Make sure this is indeed the client#with_context API
        # @return [ Array ] The operation results and server used.
        #
        # @since 3.0.0
        def execute(client)
          # if context contains a server, client yields with that server.
          client.with_context(context) do |server|
            # @todo: change wire version to a constant
            if server.wire_version >= 2
              op = WriteCommand::Delete.new(spec, :server => server)
              op.execute(client)
            else
              deletes.each do |d|
                gle = write_concern.get_last_error
                server.dispath([message(d), gle])
              end
            end
          end
        end

        private

        # The write concern to use for this operation.
        #
        # @return [ Hash ] The write concern.
        #
        # @since 3.0.0
        def write_concern
          @spec[:write_concern]
        end

        # The delete documents.
        #
        # @return [ Array ] The delete documents.
        #
        # @since 3.0.0
        def deletes
          @spec[:deletes]
        end

        # The primary server preference for the operation.
        #
        # @return [ Mongo::ServerPreference::Primary ] A primary server preference.
        #
        # @since 3.0.0
        def server_preference
          Mongo::ServerPreference.get(:primary)
        end

        # The wire protocol message for this delete operation.
        #
        # @return [ Mongo::Protocol::Delete ] Wire protocol message.
        #
        # @since 3.0.0
        def message(delete_spec = {})
          selector    = delete_spec[:q]
          delete_opts = delete_spec[:limit] == 0 ? { } : { :flags => [:single_remove] }
          Mongo::Protocol::Delete.new(db_name, coll_name, selector, delete_opts)
        end
      end
    end
  end
end
