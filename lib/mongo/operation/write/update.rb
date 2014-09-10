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

require 'mongo/operation/write/update/response'

module Mongo
  module Operation
    module Write

      # A MongoDB update operation.
      # If the server version is >= 2.5.5, a write command operation will be created
      # and sent instead.
      # See Mongo::Operation::Write::Command::Update
      #
      # @since 2.0.0
      class Update
        include Executable

        # Initialize the update operation.
        #
        # @example
        #   include Mongo
        #   include Operation
        #   Write::Update.new({ :update => { :q => { :foo => 1 },
        #                                    :u => { :$set =>
        #                                            { :bar => 1 }},
        #                                    :multi  => true,
        #                                    :upsert => false },
        #                       :db_name       => 'test',
        #                       :coll_name     => 'test_coll',
        #                       :write_concern => write_concern
        #                     })
        #
        # @param [ Hash ] spec The specifications for the update.
        #
        # @option spec :update [ hash ] The update document.
        # @option spec :db_name [ String ] The name of the database on which
        #   the operation should be executed.
        # @option spec :coll_name [ String ] The name of the collection on which
        #   the operation should be executed.
        # @option spec :write_concern [ Mongo::WriteConcern::Mode ] The write concern.
        # @option spec :options [ Hash ] Options for the command, if it ends up being a
        #   write command.
        #
        # @since 2.0.0
        def initialize(spec)
          @spec = spec
        end

        # Execute the operation.
        # If the server version is < 2.5.5, an update operation is sent.
        # If the server version is >= 2.5.5, an update write command operation is
        # created and sent instead.
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
            op = Command::Update.new(spec.merge(:updates => [ update ] ))
            Response.new(op.execute(context)).verify!
          else
            context.with_connection do |connection|
              Response.new(connection.dispatch([ message, gle ].compact)).verify!
            end
          end
        end

        private

        # The update document.
        #
        # @return [ Array ] The update document.
        #
        # @since 2.0.0
        def update
          @spec[:update]
        end

        # The wire protocol message for this update operation.
        #
        # @return [ Mongo::Protocol::Update ] Wire protocol message.
        #
        # @since 2.0.0
        def message
          update_options = update[:multi] ? { :flags => [:multi_update] } : { :flags => [ ] }
          update_options[:flags] << :upsert if update[:upsert]
          puts "update_options: #{update_options}"
          Protocol::Update.new(db_name, coll_name, update[:q], update[:u], update_options)
        end
      end
    end
  end
end
