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
        # @since 3.0.0
        module Writable

          # Initialize the write command.
          #
          # @param [ Collection ] collection The collection on which the write command
          #   will be executed.
          # @param [ Hash ] spec The specifications for the write command.
          #
          # @option spec :write_concern [ Mongo::WriteConcern::Mode ] The write concern.
          # @option spec :opts [ Hash ] Options for the command.
          #
          # @since 3.0.0
          def initialize(collection, spec)
            @collection = collection
            @spec       = spec
          end

          private

          # The write concern to use for this operation.
          #
          # @return [ Mongo::WriteConcern::Mode ] The write concern.
          #
          # @since 3.0.0
          def write_concern
            @spec[:write_concern] || DEFAULT_WRITE_CONCERN
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
          # @since 3.0.0
          def ordered?
            @spec[:ordered] ? !!@spec[:ordered] : true
          end

          # Any options for the command.
          #
          # @return [ Hash ] Command options. 
          #
          # @since 3.0.0
          def opts
            @spec[:opts]
          end

          # The wire protocol message for this write operation.
          #
          # @return [ Mongo::Protocol::Query ] Wire protocol message. 
          #
          # @since 3.0.0
          def message
            Mongo::Protocol::Query.new(db_name, Mongo::Operation::COMMAND_COLLECTION_NAME,
                                       selector, opts)
          end
        end
      end
    end
  end
end
