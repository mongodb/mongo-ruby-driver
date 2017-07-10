# Copyright (C) 2014-2017 MongoDB, Inc.
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

      # This module contains common functionality for operations that send either
      # a write command or a specific wire protocol message, depending on server version.
      # For server versions >= 2.6, a write command is sent.
      #
      # @since 2.1.0
      module WriteCommandEnabled

        # Execute the operation.
        #
        # @example Execute the operation.
        #   operation.execute(server)
        #
        # @param [ Mongo::Server ] server The server to send this operation to.
        #
        # @return [ Result ] The operation result.
        #
        # @since 2.1.0
        def execute(server)
          if !server.features.write_command_enabled? || unacknowledged_write?
            raise Error::UnsupportedCollation.new(Error::UnsupportedCollation::UNACKNOWLEDGED_WRITES_MESSAGE) if has_collation?
            execute_message(server)
          else
            execute_write_command(server)
          end
        end

        private

        def has_collation?
          false
        end

        def unacknowledged_write?
          write_concern && write_concern.get_last_error.nil?
        end

        def execute_write_command(server)
          result_class = self.class.const_defined?(:Result, false) ? self.class::Result : Result
          result_class.new(write_command_op.execute(server)).validate!
        end
      end
    end
  end
end
