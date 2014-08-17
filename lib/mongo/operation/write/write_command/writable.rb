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
      module WriteCommand

        # Provides common behavior for all write commands.
        #
        # @since 2.0.0
        module Writable

          # Initialize the write command.
          #
          # @param [ Hash ] spec The specifications for the write command.
          # @param [ Hash ] context The context for executing this operation.
          #
          # @option spec :write_concern [ Mongo::WriteConcern::Mode ] The write concern.
          # @option spec :ordered [ true, false ] Whether execution should halt after
          #   the first error encountered on the server.
          # @option spec :opts [ Hash ] Options for the command.
          #
          # @since 2.0.0
          def initialize(spec)
            @spec = spec
          end

          private

          # Whether this operation may be executed on a secondary server.
          #
          # @return [ false ] A write command may not be executed on a secondary.
          def secondary_ok?
            false
          end

          # Whether the batch writes should be applied in the same order the
          # items appear, ie. sequentially. 
          # If ordered is false, the server applies the batch items in no particular
          # order, and possibly in parallel. Execution halts after the first error.
          # The default value is true, which means the batch items are applied
          # sequentially.
          #
          # @return [ true, false ] Whether batch items are applied sequentially. 
          #
          # @since 2.0.0
          def ordered?
            @spec[:ordered] ? !!@spec[:ordered] : true
          end

          # Options for the write command.
          # A command should have limit -1.
          #
          # @return [ Hash ] Command options.
          #
          # @since 2.0.0
          def options
            return { :limit => -1 } unless @spec[:opts]
            unless @spec[:opts][:limit] && @spec[:opts][:limit] == -1
              @spec[:opts].merge(:limit => -1)
            end
          end

          # The wire protocol message for this write operation.
          #
          # @return [ Mongo::Protocol::Query ] Wire protocol message.
          #
          # @since 2.0.0
          def message
            Protocol::Query.new(db_name, Database::COMMAND, selector, options)
          end
        end
      end
    end
  end
end
