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
      # If the server version is >= 2.5.5, a write command operation will be created
      # and sent instead.
      # See Mongo::Operation::Write::WriteCommand::Update
      #
      # @since 3.0.0
      class Update
        include Executable

        # Initialize the update operation.
        #
        # @example
        #   include Mongo
        #   include Operation
        #   Write::Update.new(collection,
        #                     :updates => [{ :q => { :foo => 1 },
        #                                    :u => { :$set =>
        #                                            { :bar => 1 }},
        #                                    :multi  => true,
        #                                    :upsert => false }],
        #                     :write_concern => write_concern)
        #
        # @param [ Collection ] collection The collection in which the update will be
        #   executed.
        # @param [ Hash ] spec The specifications for the update.
        #
        # @option spec :updates [ Array ] The update documents.
        # @option spec :write_concern [ Mongo::WriteConcern::Mode ] The write concern.
        # @option spec :opts [ Hash ] Options for the command, if it ends up being a
        #   write command.
        #
        # @since 3.0.0
        def initialize(collection, spec)
          @collection = collection
          @spec       = spec
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
        # @since 3.0.0
        def execute(context)
          raise Exception, "Must use primary server" unless context.primary?
          # @todo: change wire version to constant
          if context.wire_version >= 2
            op = WriteCommand::Update.new(collection, spec.merge(:update => coll_name))
            op.execute(context)
          else
            updates.each do |d|
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
        # @since 3.0.0
        def write_concern
          @spec[:write_concern]
        end

        # The update documents.
        #
        # @return [ Array ] The update documents.
        #
        # @since 3.0.0
        def updates
          @spec[:updates]
        end

        # The wire protocol message for this update operation.
        #
        # @return [ Mongo::Protocol::Update ] Wire protocol message.
        #
        # @since 3.0.0
        def message(update_spec = {})
          selector    = update_spec[:q]
          update      = update_spec[:u]
          update_opts = update_spec[:multi] ? { :flags => [:multi_update] } : { }
          Mongo::Protocol::Update.new(db_name, coll_name, selector, update, update_opts)
        end
      end
    end
  end
end
