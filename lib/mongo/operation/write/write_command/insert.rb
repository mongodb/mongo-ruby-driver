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

        # A MongoDB insert write command operation.
        # Supported in server versions >= 2.5.5
        #
        # @example
        #   include Mongo
        #   include Operation
        #   Write::WriteCommand::Insert.new(collection,
        #                                   :documents     => [{ :foo => 1 }],
        #                                   :write_concern => write_concern)
        # @since 3.0.0
        class Insert
          include Executable
          include Writable

          private

          def secondary_ok?
            false
          end

          # The query selector for this insert command operation.
          #
          # @return [ Hash ] The selector describing this insert operation.
          #
          # @since 3.0.0
          def selector
            { :insert        => coll_name,
              :documents     => @spec[:documents],
              :write_concern => write_concern,
              :ordered       => ordered?
            }
          end
        end
      end
    end
  end
end

